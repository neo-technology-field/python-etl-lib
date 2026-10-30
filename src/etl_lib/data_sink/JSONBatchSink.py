import json
import time
from pathlib import Path
from typing import Generator

from etl_lib.core.BatchProcessor import (BatchProcessor, BatchResults,
                                         append_result)
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task

FORMATS = ("jsonl", "array")


class JSONBatchSink(BatchProcessor):
    """
    BatchProcessor that writes batches of data to a JSON file.

    The default ``jsonl`` format writes one object per line and appends to an existing file, so
    a run can be resumed or added to. The ``array`` format writes a single JSON array and always
    starts a new file, because an array cannot be extended record by record while staying valid
    JSON. In that mode the closing bracket is written when the batch generator finishes or is
    closed, so an abandoned run still leaves a parsable file.
    """

    def __init__(self, context: ETLContext, task: Task | None, predecessor: BatchProcessor, file_path: Path,
                 format: str = "jsonl", **kwargs):
        """
        Constructs a new JSONBatchSink.

        Args:
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
            predecessor: BatchProcessor which :func:`~get_batch` function will be called to receive batches to process.
            file_path: Path to the JSON file where data will be written.
            format: ``jsonl`` for one object per line in append mode, ``array`` for a single JSON array.
            **kwargs: Passed on to `json.dumps`. Note that ``indent`` produces multi-line records and
                therefore no longer valid JSON Lines.

        Raises:
            ValueError: If ``format`` is not one of the supported formats.
        """
        super().__init__(context, task, predecessor)
        if format not in FORMATS:
            raise ValueError(f"Unsupported JSON format '{format}', expected one of {FORMATS}")
        self.file_path = file_path
        self.format = format
        self.dumps_kwargs = kwargs

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        if self.predecessor is None:
            raise ValueError(f"{self.__class__.__name__} requires a predecessor")

        as_array = self.format == "array"

        with self.file_path.open(mode="w" if as_array else "a", encoding="utf-8") as f:
            if as_array:
                f.write("[")

            rows_written = 0
            try:
                for batch_result in self.predecessor.get_batch(max_batch_size):
                    t0 = time.perf_counter()
                    rows_written += self._write_batch(f, batch_result.chunk, as_array, rows_written)
                    self._instrument("json_write_batch", {
                        "rows": len(batch_result.chunk),
                        "dt_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                    })
                    yield append_result(batch_result, {"rows_written": len(batch_result.chunk)})
            finally:
                if as_array:
                    f.write("\n]\n" if rows_written else "]\n")

    def _write_batch(self, file, data: list[dict], as_array: bool, rows_before: int) -> int:
        """
        Writes one batch and returns the number of records written.

        Args:
            file: Open file handle owned by :func:`~get_batch`.
            data: A list of dictionaries representing records.
            as_array: Whether the output is a single JSON array rather than JSON Lines.
            rows_before: Records written by previous batches, used for array separators.
        """
        if not data:
            return 0

        for idx, record in enumerate(data):
            if as_array:
                file.write("\n" if rows_before == 0 and idx == 0 else ",\n")
            file.write(json.dumps(record, **self.dumps_kwargs))
            if not as_array:
                file.write("\n")

        return len(data)
