import threading
import time
from typing import Generator, List

import pytest

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor, ParallelBatchResult


class SingleWavePredecessor(BatchProcessor):
    def __init__(self, wave: ParallelBatchResult):
        super().__init__(context=None, task=None, predecessor=None)
        self._wave = wave
        self._done = False

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        if self._done:
            return
        self._done = True
        yield self._wave


class ConcurrencyTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def enter(self) -> None:
        with self._lock:
            self.active += 1
            if self.active > self.max_active:
                self.max_active = self.active

    def exit(self) -> None:
        with self._lock:
            self.active -= 1


class TrackingWorker(BatchProcessor):
    def __init__(self, tracker: ConcurrencyTracker, barrier: threading.Barrier, sleep_s: float = 0.05):
        super().__init__(context=None, task=None, predecessor=None)
        self._tracker = tracker
        self._barrier = barrier
        self._sleep_s = sleep_s

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        upstream = next(self.predecessor.get_batch(max_batch_size))
        items = upstream.chunk

        self._tracker.enter()
        try:
            self._barrier.wait(timeout=2.0)
            time.sleep(self._sleep_s)
        finally:
            self._tracker.exit()

        yield BatchResults(chunk=list(items), statistics={"processed": len(items)}, batch_size=len(items))


def test_parallel_batch_processor_runs_buckets_concurrently():
    tracker = ConcurrencyTracker()
    barrier = threading.Barrier(4)

    wave = ParallelBatchResult(
        chunk=[list(range(0, 10)), list(range(10, 20)), list(range(20, 30)), list(range(30, 40))],
        statistics={"upstream": 1},
        batch_size=40,
    )
    predecessor = SingleWavePredecessor(wave)

    pbp = ParallelBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        worker_factory=lambda: TrackingWorker(tracker, barrier),
        max_workers=4,
        prefetch=1,
    )

    out = next(pbp.get_batch(999))
    assert out.batch_size == 40
    assert tracker.max_active == 4
    assert out.statistics.get("upstream") == 1
    assert out.statistics.get("processed") == 40


def test_parallel_batch_processor_is_fail_fast():
    barrier = threading.Barrier(2)

    class FailingWorker(BatchProcessor):
        def __init__(self):
            super().__init__(context=None, task=None, predecessor=None)

        def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
            next(self.predecessor.get_batch(max_batch_size))
            barrier.wait(timeout=2.0)
            raise RuntimeError("boom")

    wave = ParallelBatchResult(
        chunk=[[1], [2]],
        statistics={},
        batch_size=2,
    )
    predecessor = SingleWavePredecessor(wave)

    pbp = ParallelBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        worker_factory=lambda: FailingWorker(),
        max_workers=2,
        prefetch=1,
    )

    with pytest.raises(RuntimeError):
        next(pbp.get_batch(999))


class ListSource(BatchProcessor):
    def __init__(self, items: List[dict], chunk_size: int):
        super().__init__(context=None, task=None, predecessor=None)
        self._items = list(items)
        self._chunk_size = chunk_size

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        idx = 0
        while idx < len(self._items):
            chunk = self._items[idx: idx + self._chunk_size]
            idx += self._chunk_size
            yield BatchResults(chunk=chunk, statistics={}, batch_size=len(chunk))


def test_splitter_and_parallel_processor_end_to_end_without_neo4j():
    from etl_lib.core.SplittingBatchProcessor import SplittingBatchProcessor

    def extractor(item: dict) -> tuple[int, int]:
        return item["r"], item["c"]

    extractor.table_size = 4
    extractor.monopartite = True

    items = [
        {"id": 1, "r": 0, "c": 1},
        {"id": 2, "r": 0, "c": 1},
        {"id": 3, "r": 2, "c": 3},
        {"id": 4, "r": 2, "c": 3},
    ]

    source = ListSource(items, chunk_size=4)
    splitter = SplittingBatchProcessor(context=None, task=None, predecessor=source, table_size=4, id_extractor=extractor)

    tracker = ConcurrencyTracker()
    lock = threading.Lock()
    processed: List[int] = []

    class CollectingWorker(BatchProcessor):
        def __init__(self):
            super().__init__(context=None, task=None, predecessor=None)

        def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
            upstream = next(self.predecessor.get_batch(max_batch_size))
            batch = upstream.chunk

            tracker.enter()
            try:
                time.sleep(0.05)
            finally:
                tracker.exit()

            with lock:
                processed.extend([row["id"] for row in batch])

            yield BatchResults(chunk=batch, statistics={"processed": len(batch)}, batch_size=len(batch))

    pbp = ParallelBatchProcessor(
        context=None,
        task=None,
        predecessor=splitter,
        worker_factory=lambda: CollectingWorker(),
        max_workers=2,
        prefetch=2,
    )

    out = next(pbp.get_batch(2))
    assert sorted(processed) == [1, 2, 3, 4]
    assert out.statistics.get("processed") == 4
    assert tracker.max_active == 2
