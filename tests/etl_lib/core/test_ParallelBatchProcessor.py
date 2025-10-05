from typing import List

import pytest
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor, ParallelBatchResult


def make_pbr(partitions: List[List[int]]) -> ParallelBatchResult:
    """
    Helper to build a ParallelBatchResult where each sub-list is a partition.
    Statistics will be empty.
    """
    return ParallelBatchResult(
        chunk=partitions,
        statistics={'a': 1, 'b': 2},
        batch_size=sum(len(p) for p in partitions),
    )


class DummyPredecessor:
    """
    Yields a fixed sequence of ParallelBatchResult batches.
    """

    def __init__(self, pbrs: List[ParallelBatchResult]):
        self._pbrs = pbrs

    def get_batch(self, max_batch_size: int):
        # ignore max_batch_size for simplicity
        for pbr in self._pbrs:
            yield pbr


class DummyWorker(BatchProcessor):
    """
    For each input batch (list of ints), returns each int multiplied by 10,
    and records statistics as count of items.
    """

    def __init__(self, context=None, task=None, predecessor=None):
        super().__init__(context, task, predecessor)

    def get_batch(self, max_batch_size: int):
        # read full predecessor batch
        for br in self.predecessor.get_batch(max_batch_size):
            transformed = [x * 10 for x in br.chunk]
            yield BatchResults(
                chunk=transformed,
                statistics={"processed": len(transformed)},
                batch_size=len(transformed)
            )


def test_single_partition_batch(etl_context):
    # one batch with one partition
    pbr = make_pbr([[1, 2, 3]])
    pred = DummyPredecessor([pbr])

    proc = ParallelBatchProcessor(
        context=etl_context,
        worker_factory=lambda: DummyWorker(context=etl_context),
        predecessor=pred,
        max_workers=2,
        prefetch=1,
    )

    br = next(proc.get_batch(max_batch_size=10))
    # Flatten multipliers: each partition * 10
    assert br.chunk == [10, 20, 30]
    assert br.statistics == {'a': 1, 'b': 2, 'processed': 3}
    assert br.batch_size == 3


def test_multiple_partitions_order_and_stats(etl_context):
    # one parallel batch with 3 partitions
    pbr = make_pbr([[1, 2], [3], [4, 5, 6]])
    pred = DummyPredecessor([pbr])

    proc = ParallelBatchProcessor(
        context=etl_context,
        worker_factory=lambda: DummyWorker(context=etl_context),
        predecessor=pred,
        max_workers=3,
        prefetch=1,
    )

    br = next(proc.get_batch(max_batch_size=10))
    # order may interleave due to concurrency; compare as sets/lists after sorting
    assert sorted(br.chunk) == sorted([10, 20, 30, 40, 50, 60])
    # statistics should sum processed counts
    assert br.statistics == {'a': 1, 'b': 2, 'processed': 6}
    assert br.batch_size == 6


def test_multiple_batches_prefetching(etl_context):
    # two PBRs to ensure prefetch queue works
    pbr1 = make_pbr([[1], [2]])
    pbr2 = make_pbr([[3, 4], [5]])
    pred = DummyPredecessor([pbr1, pbr2])

    proc = ParallelBatchProcessor(
        context=etl_context,
        worker_factory=lambda: DummyWorker(context=etl_context),
        predecessor=pred,
        max_workers=2,
        prefetch=2,
    )

    batches = list(proc.get_batch(max_batch_size=10))

    assert len(batches) == 2
    # first batch
    br1 = batches[0]
    assert sorted(br1.chunk) == [10, 20]
    assert br1.statistics == {'a': 1, 'b': 2, 'processed': 2}
    assert br1.batch_size == 2
    # second batch
    br2 = batches[1]
    assert sorted(br2.chunk) == [30, 40, 50]
    assert br2.statistics == {'a': 1, 'b': 2, 'processed': 3}
    assert br2.batch_size == 3


def test_parallel_processing_fail_fast(etl_context):
    """ParallelBatchProcessor should fail fast when a partition processing throws an exception."""
    partitions = [
        [1, 2, 3],  # Partition 1 (normal)
        [4, -1, 6],  # Partition 2 contains a marker (-1) that will cause the worker to fail
        [7, 8, 9],  # Partition 3 (normal)
    ]
    pbr = ParallelBatchResult(chunk=partitions, statistics={}, batch_size=sum(len(p) for p in partitions))
    pred = DummyPredecessor([pbr])

    class FailingWorker(BatchProcessor):
        def __init__(self, context=etl_context, predecessor=None):
            super().__init__(context, task=None, predecessor=predecessor)

        def get_batch(self, max_size):
            for batch in self.predecessor.get_batch(max_size):
                for item in batch.chunk:
                    if item == -1:
                        raise RuntimeError("Test failure: encountered -1")
                yield BatchResults(
                    chunk=batch.chunk,
                    statistics={"processed": len(batch.chunk)},
                    batch_size=len(batch.chunk),
                )

    proc = ParallelBatchProcessor(
        context=etl_context,
        worker_factory=lambda: FailingWorker(context=etl_context),
        predecessor=pred,
        max_workers=2,
        prefetch=1,
    )

    with pytest.raises(RuntimeError) as excinfo:
        list(proc.get_batch(max_batch_size=10))

    # The RuntimeError message should indicate parallel processing failed
    assert "partition processing failed" in str(excinfo.value)
    # The original exception from the worker should be preserved as __cause__
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert "encountered -1" in str(excinfo.value.__cause__)
