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
    Range validation remains the responsibility of the SplittingBatchProcessor.

    Args:
        table_size: Informational hint carried on the extractor; used by callers to sanity-check.
        start_key: Field name for the start node identifier.
        end_key: Field name for the end node identifier.

    Returns:
        Callable[[Mapping[str, Any]], tuple[int, int]]: Maps {start_key, end_key} → (row, col).
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


class SplittingBatchProcessor(BatchProcessor):
    """
    BatchProcessor that splits incoming BatchResults into non-overlapping partitions based
    on row/col indices extracted by the id_extractor, and emits full or remaining batches
    using the mix-and-batch algorithm from https://neo4j.com/blog/developer/mix-and-batch-relationship-load/
    Each emitted batch is a list of per-cell lists (array of arrays), so callers
    can process each partition (other name for a cell) in parallel.

    A batch for a schedule group is  emitted when all cells in that group have at least `batch_size` items.
    In addition, when a cell/partition reaches 3x the configured max_batch_size, the group is emitted to avoid
    overflowing the buffer when the distribution per cell is uneven.
    Leftovers are flushed after source exhaustion.
    Emitted batches never exceed the configured max_batch_size.
    """

    def __init__(self, context, table_size: int, id_extractor: Callable[[Any], Tuple[int, int]],
                 task=None, predecessor=None):
        super().__init__(context, task, predecessor)

        # If the extractor carries an expected table size, use or validate it
        if hasattr(id_extractor, "table_size"):
            expected_size = id_extractor.table_size
            if table_size is None:
                table_size = expected_size  # determine table size from extractor if not provided
            elif table_size != expected_size:
                raise ValueError(f"Mismatch between provided table_size ({table_size}) and "
                                 f"id_extractor table_size ({expected_size}).")
        elif table_size is None:
            raise ValueError("table_size must be specified if id_extractor has no defined table_size")
        self.table_size = table_size
        self._id_extractor = id_extractor

        # Initialize 2D buffer for partitions
        self.buffer: Dict[int, Dict[int, List[Any]]] = {
            i: {j: [] for j in range(self.table_size)} for i in range(self.table_size)
        }
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def _generate_batch_schedule(self) -> List[List[Tuple[int, int]]]:
        """
        Create diagonal stripes (row, col) partitions to ensure no overlapping IDs
        across emitted batches.
        Example grid:
                 ||  0  |  1  |  2
            =====++=====+=====+=====
              0  ||  0  |  1  |  2
            -----++-----+-----+----
              1  ||  2  |  0  |  1
            -----++-----+-----+-----
              2  ||  1  |  2  |  0

        would return [[(0, 0), (1, 1), (2, 2)], [(0, 1), (1, 2), (2, 0)], [(0, 2), (1, 0), (2, 1)]]
        """
        schedule: List[List[Tuple[int, int]]] = []
        for shift in range(self.table_size):
            partition: List[Tuple[int, int]] = [
                (i, (i + shift) % self.table_size)
                for i in range(self.table_size)
            ]
            schedule.append(partition)
        return schedule

    def _flush_group(
            self,
            partitions: List[Tuple[int, int]],
            batch_size: int,
            statistics: Dict[str, Any] | None = None,
    ) -> Generator[BatchResults, None, None]:
        """
        Extract up to `batch_size` items from each cell in `partitions`, remove them from the buffer,
        and yield a BatchResults whose chunks is a list of per-cell lists from these partitions.

        Args:
            partitions: Cell coordinates forming a diagonal group from the schedule.
            batch_size: Max number of items to take from each cell.
            statistics: Stats dict to attach to this emission (use {} for interim waves).
                        The "final" emission will pass the accumulated stats here.

        Notes:
            - Debug-only: prints a 2D matrix of cell sizes when logger is in DEBUG.
            - batch_size in the returned BatchResults equals the number of emitted items.
        """
        self._log_buffer_matrix(partition=partitions)

        cell_chunks: List[List[Any]] = []
        for row, col in partitions:
            q = self.buffer[row][col]
            take = min(batch_size, len(q))
            part = q[:take]
            cell_chunks.append(part)
            # remove flushed items
            self.buffer[row][col] = q[take:]

        emitted = sum(len(c) for c in cell_chunks)

        result = BatchResults(
            chunk=cell_chunks,
            statistics=statistics or {},
            batch_size=emitted,
        )
        yield result

    def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
        """
        Consume upstream batches, split data across cells, and emit diagonal partitions:
          - During streaming: emit a full partition when all its cells have >= max_batch__size.
          - Also during streaming: if any cell in a partition builds up beyond a 'burst' threshold
            (3 * of max_batch__size), emit that partition early, taking up to max_batch__size
            from each cell.
          - After source exhaustion: flush leftovers in waves capped at max_batch__size per cell.

        Statistics policy:
          * Every emission except the last carries {}.
          * The last emission carries the accumulated upstream stats (unfiltered).
        """
        schedule = self._generate_batch_schedule()

        accum_stats: Dict[str, Any] = {}
        pending: BatchResults | None = None  # hold back the most recent emission so we know what's final

        burst_threshold = 3 * max_batch__size

        for upstream in self.predecessor.get_batch(max_batch__size):
            # accumulate upstream stats (unfiltered)
            if upstream.statistics:
                accum_stats = merge_summery(accum_stats, upstream.statistics)

            # add data to cells
            for item in upstream.chunk:
                r, c = self._id_extractor(item)
                if not (0 <= r < self.table_size and 0 <= c < self.table_size):
                    raise ValueError(f"partition id out of range: {(r, c)} for table_size={self.table_size}")
                self.buffer[r][c].append(item)

            # process partitions
            for partition in schedule:
                # normal full flush when all cells are ready
                if all(len(self.buffer[r][c]) >= max_batch__size for r, c in partition):
                    br = next(self._flush_group(partition, max_batch__size, statistics={}))
                    if pending is not None:
                        yield pending
                    pending = br
                    continue

                # burst flush if any cell backlog explodes
                hot_cells = [(r, c, len(self.buffer[r][c])) for r, c in partition if
                             len(self.buffer[r][c]) >= burst_threshold]
                if hot_cells:
                    top_r, top_c, top_len = max(hot_cells, key=lambda x: x[2])
                    self.logger.debug(
                        "burst flush: partition=%s threshold=%d top_cell=(%d,%d len=%d)",
                        partition, burst_threshold, top_r, top_c, top_len
                    )
                    br = next(self._flush_group(partition, max_batch__size, statistics={}))
                    if pending is not None:
                        yield pending
                    pending = br

        # source exhausted: drain leftovers in capped waves (respecting batch size)
        self.logger.debug("start flushing leftovers")
        for partition in (p for p in schedule if any(self.buffer[r][c] for r, c in p)):
            while any(self.buffer[r][c] for r, c in partition):
                br = next(self._flush_group(partition, max_batch__size, statistics={}))
                if pending is not None:
                    yield pending
                pending = br

        # final emission carries accumulated stats once
        if pending is not None:
            yield BatchResults(chunk=pending.chunk, statistics=accum_stats, batch_size=pending.batch_size)

    def _log_buffer_matrix(self, *, partition: List[Tuple[int, int]]) -> None:
        """
        Dumps a compact 2D matrix of per-cell sizes (len of each buffer) when DEBUG is enabled.
        """
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        counts = [
            [len(self.buffer[r][c]) for c in range(self.table_size)]
            for r in range(self.table_size)
        ]
        marks = set(partition)

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
