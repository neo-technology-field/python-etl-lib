from typing import Generator, Callable, Optional
from sqlalchemy import text
from etl_lib.core.BatchProcessor import BatchResults, BatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task


class SQLBatchSource(BatchProcessor):
    def __init__(
            self,
            context: ETLContext,
            task: Task,
            query: str,
            record_transformer: Optional[Callable[[dict], dict]] = None,
            **kwargs
    ):
        """
        Constructs a new SQLBatchSource.

        Args:
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
            query: SQL query to execute.
            record_transformer: Optional function to transform each row (dict format).
            kwargs: Arguments passed as parameters with the query.
        """
        super().__init__(context, task)
        self.query = query
        self.record_transformer = record_transformer
        self.kwargs = kwargs  # Query parameters

    def __read_records(self, conn, batch_size: int):
        batch_ = []
        result = conn.execute(text(self.query), self.kwargs)  # Safe execution with bound parameters

        for row in result.mappings():  # Returns row as dict (like Neo4j's `record.data()`)
            data = dict(row)  # Convert to dictionary
            if self.record_transformer:
                data = self.record_transformer(data)
            batch_.append(data)

            if len(batch_) == batch_size:
                yield batch_
                batch_ = []  # Reset batch

        if batch_:
            yield batch_

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Fetches data in batches using an open transaction, similar to Neo4j's approach.
        """
        with self.context.sql.engine.connect() as conn:  # Keep transaction open
            with conn.begin():  # Ensures rollback on failure
                for chunk in self.__read_records(conn, max_batch_size):
                    yield BatchResults(
                        chunk=chunk,
                        statistics={"sql_rows_read": len(chunk)},
                        batch_size=len(chunk)
                    )
