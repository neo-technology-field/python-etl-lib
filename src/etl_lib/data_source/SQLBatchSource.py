import logging
from typing import Generator, Callable, Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError as SAOperationalError, DBAPIError

# Conditional import for psycopg2 to avoid crashing if not installed
try:
    from psycopg2 import OperationalError as PsycopgOperationalError
except ImportError:
    class PsycopgOperationalError(Exception):
        pass

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
        Constructs a new SQLBatchSource that streams results instead of paging them.
        """
        super().__init__(context, task)
        # Remove any trailing semicolons to prevent SQL syntax errors
        self.query = query.strip().rstrip(";")
        self.record_transformer = record_transformer
        self.kwargs = kwargs
        self.logger = logging.getLogger(__name__)

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Yield successive batches using a Server-Side Cursor (Streaming).

        This avoids 'LIMIT/OFFSET' pagination, which causes performance degradation
        on large tables. Instead, it holds a cursor open and fetches rows incrementally.
        """

        with self.context.sql.engine.connect() as conn:

            conn = conn.execution_options(stream_results=True)

            try:
                self.logger.info("Starting SQL Result Stream...")

                result_proxy = conn.execute(text(self.query), self.kwargs)

                chunk = []
                count = 0

                for row in result_proxy.mappings():
                    item = self.record_transformer(dict(row)) if self.record_transformer else dict(row)
                    chunk.append(item)
                    count += 1

                    # Yield when batch is full
                    if len(chunk) >= max_batch_size:
                        yield BatchResults(
                            chunk=chunk,
                            statistics={"sql_rows_read": len(chunk)},
                            batch_size=len(chunk),
                        )
                        chunk = []  # Clear memory

                # Yield any remaining rows
                if chunk:
                    yield BatchResults(
                        chunk=chunk,
                        statistics={"sql_rows_read": len(chunk)},
                        batch_size=len(chunk),
                    )

                self.logger.info(f"SQL Stream finished. Total rows read: {count}")

            except (PsycopgOperationalError, SAOperationalError, DBAPIError) as err:
                self.logger.error(f"Stream failed: {err}")
                raise
