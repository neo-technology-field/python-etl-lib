import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Generator, List

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.utils import merge_summery


class ParallelBatchResult(BatchResults):
    """
    Represents one *wave* produced by the splitter.

    `chunk` is a list of bucket-batches. Each sub-list is processed by one worker instance.
    """
    pass


class ParallelBatchProcessor(BatchProcessor):
    """
    BatchProcessor that runs a worker over the bucket-batches of each ParallelBatchResult
    in parallel threads, while prefetching the next ParallelBatchResult from its predecessor.

    Note:
        - The predecessor must emit `ParallelBatchResult` instances (waves).
        - This processor collects the BatchResults from all workers for one wave and merges them
          into one BatchResults.
        - The returned BatchResults will not obey the max_batch_size from get_batch() because
          it represents the full wave.

    Args:
        context: ETL context.
        worker_factory: A zero-arg callable that returns a new BatchProcessor each time it's called.
        task: optional Task for reporting.
        predecessor: upstream BatchProcessor that must emit ParallelBatchResult See
                :class:`~etl_lib.core.SplittingBatchProcessor.SplittingBatchProcessor`.
        max_workers: number of parallel threads for bucket processing.
        prefetch: number of waves to prefetch.

    Behavior:
        - For every wave, spins up `max_workers` threads.
        - Each thread processes one bucket-batch using a fresh worker from `worker_factory()`.
        - Collects and merges worker results in a fail-fast manner.
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

    def _process_wave(self, wave: ParallelBatchResult) -> BatchResults:
        """
        Process one wave: run one worker per bucket-batch and merge their BatchResults.

        Statistics:
            `wave.statistics` is used as the initial merged stats, then merged with each worker's stats.
        """
        merged_stats = dict(wave.statistics or {})
        merged_chunk = []
        total = 0

        self.logger.debug(f"Processing wave with {len(wave.chunk)} buckets")

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="PBP_worker_") as pool:
            futures = [pool.submit(self._process_bucket_batch, bucket_batch) for bucket_batch in wave.chunk]
            try:
                for f in as_completed(futures):
                    out = f.result()
                    merged_stats = merge_summery(merged_stats, out.statistics or {})
                    total += out.batch_size
                    merged_chunk.extend(out.chunk if isinstance(out.chunk, list) else [out.chunk])
            except Exception as e:
                for g in futures:
                    g.cancel()
                pool.shutdown(cancel_futures=True)
                raise RuntimeError("bucket processing failed") from e

        self.logger.debug(f"Finished wave with stats={merged_stats}")
        return BatchResults(chunk=merged_chunk, statistics=merged_stats, batch_size=total)

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Pull waves from the predecessor (prefetching up to `prefetch` ahead), process each wave's
        buckets in parallel, and yield one flattened BatchResults per wave.
        """
        wave_queue: queue.Queue[ParallelBatchResult | object] = queue.Queue(self.prefetch)
        SENTINEL = object()
        exc: BaseException | None = None

        def producer():
            nonlocal exc
            try:
                for wave in self.predecessor.get_batch(max_batch_size):
                    self.logger.debug(
                        f"adding wave stats={wave.statistics} buckets={len(wave.chunk)} to queue size={wave_queue.qsize()}"
                    )
                    wave_queue.put(wave)
            except BaseException as e:
                exc = e
            finally:
                wave_queue.put(SENTINEL)

        threading.Thread(target=producer, daemon=True, name="prefetcher").start()

        while True:
            wave = wave_queue.get()
            if wave is SENTINEL:
                if exc is not None:
                    self.logger.error("Upstream producer failed", exc_info=True)
                    raise exc
                break
            yield self._process_wave(wave)

    class SingleBatchWrapper(BatchProcessor):
        """
        Simple BatchProcessor that returns exactly one batch (the bucket-batch passed in via init).
        Used as predecessor for the per-bucket worker.
        """

        def __init__(self, context, batch: List[Any]):
            super().__init__(context=context, predecessor=None)
            self._batch = batch

        def get_batch(self, max_size: int) -> Generator[BatchResults, None, None]:
            yield BatchResults(
                chunk=self._batch,
                statistics={},
                batch_size=len(self._batch),
            )

    def _process_bucket_batch(self, bucket_batch):
        """
        Process one bucket-batch by running a fresh worker over it.
        """
        self.logger.debug(f"Processing batch w/ size {len(bucket_batch)}")
        wrapper = self.SingleBatchWrapper(self.context, bucket_batch)
        worker = self.worker_factory()
        worker.predecessor = wrapper
        result = next(worker.get_batch(len(bucket_batch)))
        self.logger.debug(f"Finished bucket batch stats={result.statistics}")
        return result
