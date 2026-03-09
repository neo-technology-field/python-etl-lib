import logging
import time
from pathlib import Path
from typing import Generator, Optional

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task


class ParquetBatchSource(BatchProcessor):
    """
    BatchProcessor that reads a Parquet file using pyarrow.

    The returned batch of rows will have an additional `_row` column, containing the source row of the data,
    starting with 0.
    """

    def __init__(self, file: Path, context: ETLContext, task: Optional[Task] = None, **kwargs):
        """
        Constructs a new ParquetBatchSource.

        Args:
            file: Path to the Parquet file.
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            kwargs: Will be passed on to the `pyarrow.parquet.ParquetFile.iter_batches` method.
        """
        super().__init__(context, task)
        if pq is None:
            raise ImportError("pyarrow is required for ParquetBatchSource. Install with 'pip install .[parquet]'")
        self.file = file
        self.kwargs = kwargs
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    @staticmethod
    def get_total_rows(file: Path) -> int:
        if pq is None:
            raise ImportError("pyarrow is required. Install with 'pip install .[parquet]'")
        return pq.ParquetFile(file).metadata.num_rows

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        parquet_file = pq.ParquetFile(self.file)

        batch_iter = parquet_file.iter_batches(batch_size=max_batch_size, **self.kwargs)

        row_counter = 0
        t0 = time.perf_counter()

        for batch in batch_iter:
            rows = batch.to_pylist()

            for i, row in enumerate(rows):
                row["_row"] = row_counter + i

            batch_len = len(rows)
            row_counter += batch_len

            self._instrument("parquet_read_batch", {
                "rows": batch_len,
                "dt_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            })
            t0 = time.perf_counter()

            yield BatchResults(
                chunk=rows,
                statistics={"parquet_rows_read": batch_len},
                batch_size=batch_len,
            )
