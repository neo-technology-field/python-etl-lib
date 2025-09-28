import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Generator, List

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.utils import merge_summery


class ParallelBatchResult(BatchResults):
    """
    Represents a batch split into parallelizable partitions.

    `chunk` is a list of lists, each sub-list is a partition.
    """
    pass


class ParallelBatchProcessor(BatchProcessor):
    """
    BatchProcessor that runs worker threads over partitions of batches.

    Receives a special BatchResult (:py:class:`ParallelBatchResult`) from the predecessor.
    All chunks in a ParallelBatchResult it receives can be processed in parallel.
    See :py:class:`etl_lib.core.SplittingBatchProcessor` on how to produce them.
    Prefetches the next ParallelBatchResults from its predecessor.
    The actual processing of the batches is deferred to the configured worker.

    Note:
        - The predecessor must emit `ParallelBatchResult` instances.

    Args:
        context: ETL context.
        worker_factory: A zero-arg callable that returns a new BatchProcessor
                        each time it's called. This processor is responsible for the processing pf the batches.
        task: optional Task for reporting.
        predecessor: upstream BatchProcessor that must emit ParallelBatchResult.
        max_workers: number of parallel threads for partitions.
        prefetch: number of ParallelBatchResults to prefetch from the predecessor.

    Behavior:
        - For every ParallelBatchResult, spins up `max_workers` threads.
        - Each thread calls its own worker from `worker_factory()`, with its
          partition wrapped by `SingleBatchWrapper`.
        - Collects and merges their BatchResults in a fail-fast manner: on first
          exception, logs the error, cancels remaining threads, and raises an exception.
    """

    def __init__(
            self,
            context,
            worker_factory: Callable[[], BatchProcessor],
            task=None,
            predecessor=None,
            max_workers: int = 4,
            prefetch: int = 4,
    ):
        super().__init__(context, task, predecessor)
        self.worker_factory = worker_factory
        self.max_workers = max_workers
        self.prefetch = prefetch
        self._batches_done = 0

    def _process_parallel(self, pbr: ParallelBatchResult) -> BatchResults:
        """
        Run one worker per partition in `pbr.chunk`, merge their outputs, and include upstream
        statistics from `pbr.statistics` so counters (e.g., valid/invalid rows from validation)
        are preserved through the parallel stage.

        Progress reporting:
          - After each partition completes, report batch count only
        """
        merged_stats = dict(pbr.statistics or {})
        merged_chunk = []
        total = 0

        parts_total = len(pbr.chunk)
        partitions_done = 0

        self.logger.debug(f"Processing pbr of len {parts_total}")
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='PBP_worker_') as pool:
            futures = [pool.submit(self._process_partition, part) for part in pbr.chunk]
            try:
                for f in as_completed(futures):
                    out = f.result()

                    # Merge into this PBR's cumulative result (returned downstream)
                    merged_stats = merge_summery(merged_stats, out.statistics or {})
                    total += out.batch_size
                    merged_chunk.extend(out.chunk if isinstance(out.chunk, list) else [out.chunk])

                    partitions_done += 1
                    self.context.reporter.report_progress(
                        task=self.task,
                        batches=self._batches_done,
                        expected_batches=None,
                        stats={},
                    )

            except Exception as e:
                for g in futures:
                    g.cancel()
                pool.shutdown(cancel_futures=True)
                raise RuntimeError("partition processing failed") from e

        self.logger.debug(f"Finished processing pbr with {merged_stats}")
        return BatchResults(chunk=merged_chunk, statistics=merged_stats, batch_size=total)

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Pulls ParallelBatchResult batches from the predecessor, prefetching
        up to `prefetch` ahead, processes each batch's partitions in
        parallel threads, and yields a flattened BatchResults. The predecessor
        can run ahead while the current batch is processed.
        """
        pbr_queue: queue.Queue[ParallelBatchResult | object] = queue.Queue(self.prefetch)
        SENTINEL = object()
        exc: BaseException | None = None

        def producer():
            nonlocal exc
            try:
                for pbr in self.predecessor.get_batch(max_batch_size):
                    self.logger.debug(
                        f"adding pgr {pbr.statistics} / {len(pbr.chunk)} to queue of size {pbr_queue.qsize()}"
                    )
                    pbr_queue.put(pbr)
            except BaseException as e:
                exc = e
            finally:
                pbr_queue.put(SENTINEL)

        threading.Thread(target=producer, daemon=True, name='prefetcher').start()

        while True:
            pbr = pbr_queue.get()
            if pbr is SENTINEL:
                if exc is not None:
                    self.logger.error("Upstream producer failed", exc_info=True)
                    raise exc
                break
            result = self._process_parallel(pbr)
            yield result

    class SingleBatchWrapper(BatchProcessor):
        """
        Simple BatchProcessor that returns the batch it receives via init.
        Will be used as predecessor for the worker
        """

        def __init__(self, context, batch: List[Any]):
            super().__init__(context=context, predecessor=None)
            self._batch = batch

        def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
            # Ignores max_size; yields exactly one BatchResults containing the whole batch
            yield BatchResults(
                chunk=self._batch,
                statistics={},
                batch_size=len(self._batch)
            )

    def _process_partition(self, partition):
        """
        Processes one partition of items by:
          1. Wrapping it in SingleBatchWrapper
          2. Instantiating a fresh worker via worker_factory()
          3. Setting the worker's predecessor to the wrapper
          4. Running exactly one batch and returning its BatchResults

        Raises whatever exception the worker raises, allowing _process_parallel
        to handle fail-fast behavior.
        """
        self.logger.debug("Processing partition")
        wrapper = self.SingleBatchWrapper(self.context, partition)
        worker = self.worker_factory()
        worker.predecessor = wrapper
        result = next(worker.get_batch(len(partition)))
        self.logger.debug(f"finished processing partition with {result.statistics}")
        return result
