from typing import Generator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result
from etl_lib.core.Task import Task


class ClosedLoopBatchProcessor(BatchProcessor):

    def __init__(self, context: ETLContext, task: Task, predecessor: BatchProcessor, expected_rows: int = None):
        super().__init__(context, task, predecessor)
        self.expected_rows = expected_rows

    def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None
        batch_cnt = 0
        result = BatchResults(chunk=[], statistics={}, batch_size=max_batch__size)
        for batch in self.predecessor.get_batch(max_batch__size):
            result = append_result(result, batch.statistics)
            batch_cnt += 1
            self.context.reporter.report_progress(self.task, batch_cnt, self.expected_rows, result.statistics)

        self.logger.debug(result.statistics)
        yield result
