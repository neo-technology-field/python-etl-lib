import logging
from typing import Any, Callable, Dict, Generator, List, Tuple

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.utils import merge_summery
from tabulate import tabulate


def tuple_id_extractor(table_size: int = 10) -> Callable[[Tuple[str | int, str | int]], Tuple[int, int]]:
    """
    Create an ID extractor function for tuple items, using the last decimal digit of each element.
    The output is a `(row, col)` tuple within a `table_size x table_size` grid (default 10x10).

    Args:
        table_size: The dimension of the grid (number of rows/cols). Defaults to 10.

    Returns:
        A callable that maps a tuple `(a, b)` to a tuple `(row, col)` using the last digit of `a` and `b`.

    Notes:
        This extractor does not validate the returned indices. Range validation is performed by
        :class:`~etl_lib.core.SplittingBatchProcessor.SplittingBatchProcessor`.
    """

    def extractor(item: Tuple[Any, Any]) -> Tuple[int, int]:
        a, b = item
        try:
            row = int(str(a)[-1])
            col = int(str(b)[-1])
        except Exception as e:
            raise ValueError(f"Failed to extract ID from item {item}: {e}")
        return row, col

    extractor.table_size = table_size
    return extractor


def dict_id_extractor(
        table_size: int = 10,
        start_key: str = "start",
        end_key: str = "end",
) -> Callable[[Dict[str, Any]], Tuple[int, int]]:
    """
    Build an ID extractor for dict rows. The extractor reads two fields (configurable via
    `start_key` and `end_key`) and returns (row, col) based on the last decimal digit of each.

    Args:
        table_size: Informational hint carried on the extractor.
        start_key: Field name for the start node identifier.
        end_key: Field name for the end node identifier.

    Returns:
        Callable that maps {start_key, end_key} → (row, col).
    """

    def extractor(item: Dict[str, Any]) -> Tuple[int, int]:
        missing = [k for k in (start_key, end_key) if k not in item]
        if missing:
            raise KeyError(f"Item missing required keys: {', '.join(missing)}")
        try:
            row = int(str(item[start_key])[-1])
            col = int(str(item[end_key])[-1])
        except Exception as e:
            raise ValueError(f"Failed to extract ID from item {item}: {e}")
        return row, col

    extractor.table_size = table_size
    return extractor


def canonical_integer_id_extractor(
        table_size: int = 10,
        start_key: str = "start",
        end_key: str = "end",
) -> Callable[[Dict[str, Any]], Tuple[int, int]]:
    """
    ID extractor for integer IDs with canonical folding.

    - Uses Knuth's multiplicative hashing to scatter sequential integers across the grid.
    - Canonical folding ensures (A,B) and (B,A) land in the same bucket by folding the lower
      triangle into the upper triangle (row <= col).

    The extractor marks itself as mono-partite by setting `extractor.monopartite = True`.
    """
    MAGIC = 2654435761

    def extractor(item: Dict[str, Any]) -> Tuple[int, int]:
        try:
            s_val = item[start_key]
            e_val = item[end_key]

            s_hash = (s_val * MAGIC) & 0xffffffff
            e_hash = (e_val * MAGIC) & 0xffffffff

            row = s_hash % table_size
            col = e_hash % table_size

            if row > col:
                row, col = col, row

            return row, col
        except KeyError:
            raise KeyError(f"Item missing keys: {start_key} or {end_key}")
        except Exception as e:
            raise ValueError(f"Failed to extract ID: {e}")

    extractor.table_size = table_size
    extractor.monopartite = True
    return extractor


class SplittingBatchProcessor(BatchProcessor):
    """
    Streaming wave scheduler for mix-and-batch style loading.

    Incoming rows are assigned to buckets via an `id_extractor(item) -> (row, col)` inside a
    `table_size x table_size` grid. The processor emits waves; each wave contains bucket-batches
    that are safe to process concurrently under the configured non-overlap rule.

    Non-overlap rules
    -----------------
    - Bi-partite (default): within a wave, no two buckets share a row index and no two buckets share a col index.
    - Mono-partite: within a wave, no node index is used more than once (row/col indices are the same domain).
      Enable by setting `id_extractor.monopartite = True` (as done by `canonical_integer_id_extractor`).

    Emission strategy
    -----------------
    - During streaming: emit a wave when at least one bucket is full (>= max_batch_size).
      The wave is then filled with additional non-overlapping buckets that are near-full to
      keep parallelism high without producing tiny batches.
    - If a bucket backlog grows beyond a burst threshold, emit a burst wave to bound memory.
    - After source exhaustion: flush leftovers in capped waves (max_batch_size per bucket).

    Statistics policy
    -----------------
    - Every emission except the last carries {}.
    - The last emission carries the accumulated upstream statistics (unfiltered).
    """

    def __init__(
            self,
            context,
            table_size: int,
            id_extractor: Callable[[Any], Tuple[int, int]],
            task=None,
            predecessor=None,
            near_full_ratio: float = 0.85,
            burst_multiplier: int = 25,
    ):
        super().__init__(context, task, predecessor)

        if hasattr(id_extractor, "table_size"):
            expected_size = id_extractor.table_size
            if table_size is None:
                table_size = expected_size
            elif table_size != expected_size:
                raise ValueError(
                    f"Mismatch between provided table_size ({table_size}) and id_extractor table_size ({expected_size})."
                )
        elif table_size is None:
            raise ValueError("table_size must be specified if id_extractor has no defined table_size")

        if not (0 < near_full_ratio <= 1.0):
            raise ValueError(f"near_full_ratio must be in (0, 1], got {near_full_ratio}")
        if burst_multiplier < 1:
            raise ValueError(f"burst_multiplier must be >= 1, got {burst_multiplier}")

        self.table_size = table_size
        self._id_extractor = id_extractor
        self._monopartite = bool(getattr(id_extractor, "monopartite", False))

        self.near_full_ratio = float(near_full_ratio)
        self.burst_multiplier = int(burst_multiplier)

        self.buffer: Dict[int, Dict[int, List[Any]]] = {
            r: {c: [] for c in range(self.table_size)}
            for r in range(self.table_size)
        }
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def _bucket_claims(self, row: int, col: int) -> Tuple[Any, ...]:
        """
        Return the resource claims a bucket consumes within a wave.

        - Bi-partite: claims (row-slot, col-slot)
        - Mono-partite: claims node indices touched by the bucket
        """
        if self._monopartite:
            return (row,) if row == col else (row, col)
        return ("row", row), ("col", col)

    def _all_bucket_sizes(self) -> List[Tuple[int, int, int]]:
        """
        Return all non-empty buckets as (size, row, col).
        """
        out: List[Tuple[int, int, int]] = []
        for r in range(self.table_size):
            for c in range(self.table_size):
                n = len(self.buffer[r][c])
                if n > 0:
                    out.append((n, r, c))
        return out

    def _select_wave(self, *, min_bucket_len: int, seed: List[Tuple[int, int]] | None = None) -> List[Tuple[int, int]]:
        """
        Greedy wave scheduler: pick a non-overlapping set of buckets with len >= min_bucket_len.

        If `seed` is provided, it is taken as fixed and the wave is extended greedily.
        """
        candidates: List[Tuple[int, int, int]] = []
        for r in range(self.table_size):
            for c in range(self.table_size):
                n = len(self.buffer[r][c])
                if n >= min_bucket_len:
                    candidates.append((n, r, c))

        if not candidates and not seed:
            return []

        candidates.sort(key=lambda x: (-x[0], x[1], x[2]))

        used: set[Any] = set()
        wave: List[Tuple[int, int]] = []

        if seed:
            for r, c in seed:
                claims = self._bucket_claims(r, c)
                used.update(claims)
                wave.append((r, c))

        for _, r, c in candidates:
            if (r, c) in wave:
                continue
            claims = self._bucket_claims(r, c)
            if any(claim in used for claim in claims):
                continue
            wave.append((r, c))
            used.update(claims)
            if len(wave) >= self.table_size:
                break

        return wave

    def _find_hottest_bucket(self, *, threshold: int) -> Tuple[int, int, int] | None:
        """
        Find the single hottest bucket (largest backlog) whose size is >= threshold.
        Returns (row, col, size) or None.
        """
        best: Tuple[int, int, int] | None = None
        for r in range(self.table_size):
            for c in range(self.table_size):
                n = len(self.buffer[r][c])
                if n < threshold:
                    continue
                if best is None or n > best[2]:
                    best = (r, c, n)
        return best

    def _flush_wave(
            self,
            wave: List[Tuple[int, int]],
            max_batch_size: int,
            statistics: Dict[str, Any] | None = None,
    ) -> BatchResults:
        """
        Extract up to `max_batch_size` items from each bucket in `wave`, remove them from the buffer,
        and return a BatchResults whose chunk is a list of per-bucket lists (aligned with `wave`).
        """
        self._log_buffer_matrix(wave=wave)

        bucket_batches: List[List[Any]] = []
        for r, c in wave:
            q = self.buffer[r][c]
            take = min(max_batch_size, len(q))
            bucket_batches.append(q[:take])
            self.buffer[r][c] = q[take:]

        emitted = sum(len(b) for b in bucket_batches)

        return BatchResults(
            chunk=bucket_batches,
            statistics=statistics or {},
            batch_size=emitted,
        )

    def _log_buffer_matrix(self, *, wave: List[Tuple[int, int]]) -> None:
        """
        Dumps a compact 2D matrix of per-bucket sizes (len of each buffer) when DEBUG is enabled.
        """
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        counts = [
            [len(self.buffer[r][c]) for c in range(self.table_size)]
            for r in range(self.table_size)
        ]
        marks = set(wave)

        pad = max(2, len(str(self.table_size - 1)))
        col_headers = [f"c{c:0{pad}d}" for c in range(self.table_size)]

        rows = []
        for r in range(self.table_size):
            row_label = f"r{r:0{pad}d}"
            row_vals = [f"[{v}]" if (r, c) in marks else f"{v}" for c, v in enumerate(counts[r])]
            rows.append([row_label, *row_vals])

        table = tabulate(
            rows,
            headers=["", *col_headers],
            tablefmt="psql",
            stralign="right",
            disable_numparse=True,
        )
        self.logger.debug("buffer matrix:\n%s", table)

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Consume upstream batches, bucket incoming rows, and emit waves of non-overlapping buckets.

        Streaming behavior:
          - If at least one bucket is full (>= max_batch_size), emit a wave seeded with full buckets
            and extended with near-full buckets to keep parallelism high.
          - If a bucket exceeds a burst threshold, emit a burst wave (seeded by the hottest bucket)
            and extended with near-full buckets.

        End-of-stream behavior:
          - Flush leftovers in capped waves (max_batch_size per bucket).

        Statistics policy:
          * Every emission except the last carries {}.
          * The last emission carries the accumulated upstream stats (unfiltered).
        """
        if self.predecessor is None:
            return

        accum_stats: Dict[str, Any] = {}
        pending: BatchResults | None = None

        near_full_threshold = max(1, int(max_batch_size * self.near_full_ratio))
        burst_threshold = self.burst_multiplier * max_batch_size

        for upstream in self.predecessor.get_batch(max_batch_size):
            if upstream.statistics:
                accum_stats = merge_summery(accum_stats, upstream.statistics)

            for item in upstream.chunk:
                r, c = self._id_extractor(item)
                if self._monopartite and r > c:
                    r, c = c, r
                if not (0 <= r < self.table_size and 0 <= c < self.table_size):
                    raise ValueError(f"bucket id out of range: {(r, c)} for table_size={self.table_size}")
                self.buffer[r][c].append(item)

            while True:
                full_seed = self._select_wave(min_bucket_len=max_batch_size)
                if not full_seed:
                    break
                wave = self._select_wave(min_bucket_len=near_full_threshold, seed=full_seed)
                br = self._flush_wave(wave, max_batch_size, statistics={})
                if pending is not None:
                    yield pending
                pending = br

            while True:
                hot = self._find_hottest_bucket(threshold=burst_threshold)
                if hot is None:
                    break
                hot_r, hot_c, hot_n = hot
                wave = self._select_wave(min_bucket_len=near_full_threshold, seed=[(hot_r, hot_c)])
                self.logger.debug(
                    "burst flush: hottest_bucket=(%d,%d len=%d) threshold=%d near_full_threshold=%d wave_size=%d",
                    hot_r, hot_c, hot_n, burst_threshold, near_full_threshold, len(wave)
                )
                br = self._flush_wave(wave, max_batch_size, statistics={})
                if pending is not None:
                    yield pending
                pending = br

        self.logger.debug("start flushing leftovers")
        while True:
            wave = self._select_wave(min_bucket_len=1)
            if not wave:
                break
            br = self._flush_wave(wave, max_batch_size, statistics={})
            if pending is not None:
                yield pending
            pending = br

        if pending is not None:
            yield BatchResults(chunk=pending.chunk, statistics=accum_stats, batch_size=pending.batch_size)
