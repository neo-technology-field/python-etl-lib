import csv
from pathlib import Path
from typing import Generator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result
from etl_lib.core.Task import Task


class CSVBatchSink(BatchProcessor):
    """
    BatchProcessor to write batches of data to a CSV file.
    """

    def __init__(self, context: ETLContext, task: Task, predecessor: BatchProcessor, file_path: Path, **kwargs):
        """
        Constructs a new CSVBatchSink.

        Args:
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
            predecessor: BatchProcessor which :func:`~get_batch` function will be called to receive batches to process.
            file_path: Path to the CSV file where data will be written. If the file exists, data will be appended.
            **kwargs: Additional arguments passed to `csv.DictWriter` to allow tuning the csv creation.
        """
        super().__init__(context, task, predecessor)
        self.file_path = file_path
        self.file_initialized = False
        self.csv_kwargs = kwargs

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        for batch_result in self.predecessor.get_batch(batch_size):
            self._write_to_csv(batch_result.chunk)
            yield append_result(batch_result, {"rows_written": len(batch_result.chunk)})

    def _write_to_csv(self, data: list[dict]):
        """
        Writes a batch of data to the CSV file.

        Args:
            data: A list of dictionaries representing rows of data.
        """
        if not data:
            return

        fieldnames = data[0].keys()
        write_header = not self.file_initialized or not self.file_path.exists()

        with self.file_path.open(mode="a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, **self.csv_kwargs)
            if write_header:
                writer.writeheader()
            writer.writerows(data)

        self.file_initialized = True
