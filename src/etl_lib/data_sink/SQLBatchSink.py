from typing import Generator
from sqlalchemy import text
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result
from etl_lib.core.Task import Task


class SQLBatchSink(BatchProcessor):
    """
    BatchProcessor to write batches of data to an SQL database.
    """

    def __init__(self, context: ETLContext, task: Task, predecessor: BatchProcessor, query: str):
        """
        Constructs a new SQLBatchSink.

        Args:
            context: ETLContext instance.
            task: Task instance owning this batchProcessor.
            predecessor: BatchProcessor which `get_batch` function will be called to receive batches to process.
            query: SQL query to write data.
                Data will be passed as a batch using parameterized statements (`:param_name` syntax).
        """
        super().__init__(context, task, predecessor)
        self.query = query
        self.engine = context.sql.engine

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        with self.engine.connect() as conn:
            with conn.begin():
                for batch_result in self.predecessor.get_batch(batch_size):
                    conn.execute(text(self.query), batch_result.chunk)
                    yield append_result(batch_result, {"sql_rows_written": len(batch_result.chunk)})

