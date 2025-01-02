from typing import Generator

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result


class ClosedLoopBatchProcessor(BatchProcessor):

    def __init__(self, predecessor: BatchProcessor):
        super().__init__(predecessor)

    def get_batch(self, batch_size: int)  -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        result = BatchResults([])
        for batch in self.predecessor.get_batch(batch_size):
            self.logger.debug(batch.statistics)
            result = append_result(result, batch.statistics)

        self.logger.debug(result.statistics)
        yield result
