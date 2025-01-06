from typing import Generator

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result


class ClosedLoopBatchProcessor(BatchProcessor):

    def __init__(self, predecessor: BatchProcessor):
        super().__init__(predecessor)

    def get_batch(self, max_batch__size: int)  -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        result = BatchResults(chunk=[], statistics={}, batch_size=max_batch__size)
        for batch in self.predecessor.get_batch(max_batch__size):
            self.logger.debug(batch.statistics)
            result = append_result(result, batch.statistics)

        self.logger.debug(result.statistics)
        yield result
