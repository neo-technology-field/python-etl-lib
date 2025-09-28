import time
from typing import Generator, Callable, Optional, List, Dict

from psycopg2 import OperationalError as PsycopgOperationalError
from sqlalchemy import text
from sqlalchemy.exc import OperationalError as SAOperationalError, DBAPIError

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
        self.query = query.strip().rstrip(";")
        self.record_transformer = record_transformer
        self.kwargs = kwargs

    def _fetch_page(self, limit: int, offset: int) -> Optional[List[Dict]]:
        """
        Fetch a single batch of rows using LIMIT/OFFSET, with retry/backoff.

        Each page is executed in its own transaction. On transient
        disconnects or DB errors, it retries up to 3 times with exponential backoff.

        Args:
            limit: maximum number of rows to return.
            offset: number of rows to skip before starting this page.

        Returns:
            A list of row dicts (after applying record_transformer, if any),
            or None if no rows are returned.

        Raises:
            Exception: re-raises the last caught error on final failure.
        """
        paged_sql = f"{self.query} LIMIT :limit OFFSET :offset"
        params = {**self.kwargs, "limit": limit, "offset": offset}
        max_retries = 5
        backoff = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                with self.context.sql.engine.connect() as conn:
                    with conn.begin():
                        rows = conn.execute(text(paged_sql), params).mappings().all()
                result = [
                    self.record_transformer(dict(r)) if self.record_transformer else dict(r)
                    for r in rows
                ]
                return result if result else None

            except (PsycopgOperationalError, SAOperationalError, DBAPIError) as err:

                if attempt == max_retries:
                    self.logger.error(
                        f"Page fetch failed after {max_retries} attempts "
                        f"(limit={limit}, offset={offset}): {err}"
                    )
                    raise

                self.logger.warning(
                    f"Transient DB error on page fetch {attempt}/{max_retries}: {err!r}, "
                    f"retrying in {backoff:.1f}s"
                )
                time.sleep(backoff)
                backoff *= 2

        return None

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        """
        Yield successive batches until the query is exhausted.

        Calls _fetch_page() repeatedly, advancing the offset by the
        number of rows returned. Stops when no more rows are returned.

        Args:
            max_batch_size: upper limit on rows per batch.

        Yields:
            BatchResults for each non-empty page.
        """
        offset = 0
        while True:
            chunk = self._fetch_page(max_batch_size, offset)
            if not chunk:
                break

            yield BatchResults(
                chunk=chunk,
                statistics={"sql_rows_read": len(chunk)},
                batch_size=len(chunk),
            )

            offset += len(chunk)
