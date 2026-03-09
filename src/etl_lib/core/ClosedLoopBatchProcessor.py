from typing import Generator

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task


class ClosedLoopBatchProcessor(BatchProcessor):
    """
    Reporting implementation of a BatchProcessor.

    Meant to be the last entry in the list of :py:class:`etl_lib.core.BatchProcessor` driving the processing and
    reporting updates of the processed batches using the :py:class:`etl_lib.core.ProgressReporter` from the context.
    """

    def __init__(self,
                 context: ETLContext,
                 task: Task,
                 predecessor: BatchProcessor,
                 expected_rows: int = None,
                 expected_batches: int = None):
        super().__init__(context, task, predecessor)
        self.expected_rows = expected_rows
        self.expected_batches = expected_batches

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None
        batch_cnt = 0
        result = BatchResults(chunk=[], statistics={}, batch_size=max_batch_size)
        for batch in self.predecessor.get_batch(max_batch_size):
            result = append_result(result, batch.statistics)
            batch_cnt += 1
            self.context.reporter.report_progress(self.task, batch_cnt, self._safe_calculate_count(max_batch_size),
                                                  result.statistics)

        self.logger.debug(result.statistics)
        yield result

    def _safe_calculate_count(self, batch_size: int):
        if self.expected_batches is not None:
            return self.expected_batches
        if not self.expected_rows or not batch_size:
            return 0
        return self.expected_rows + batch_size - 1
