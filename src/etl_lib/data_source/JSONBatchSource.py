import gzip
import json
import time
from pathlib import Path
from typing import Generator

try:
    import ijson
except ImportError:
    ijson = None

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.Task import Task


class JSONBatchSource(BatchProcessor):
    """
    BatchProcessor that reads a JSON file in batches.

    Two layouts are accepted and detected from the file content: JSON Lines, one object per
    line, and a single JSON array of objects. JSON Lines needs nothing beyond the standard
    library. Arrays are streamed with `ijson`, so the file is not loaded whole, which costs a
    dependency (``pip install "neo4j-etl-lib[json]"``).

    Every record gains an additional ``_row`` field holding the source row number, starting
    at 0. The file may be gzipped.
    """

    def __init__(self, context, task: Task | None, json_file: Path):
        """
        Constructs a new JSONBatchSource.

        Args:
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :class:`etl_lib.core.Task.Task` instance owning this processor. Pass `None` if the
                source is used outside a task, for example in a test.
            json_file: Path to the JSON file. Gzipped files are detected by the ``.gz`` suffix.

        Raises:
            ImportError: If the file holds a JSON array and `ijson` is not installed.
        """
        super().__init__(context, task)
        self.json_file = json_file
        self._is_array = self._starts_with_array(json_file)
        if self._is_array and ijson is None:
            raise ImportError(
                "ijson is required to read JSON arrays. Install with 'pip install \"neo4j-etl-lib[json]\"'"
            )

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        t0 = time.perf_counter()
        for batch_size, chunks_ in self._iter_batches(max_batch_size):
            self._instrument("json_read_batch", {
                "rows": batch_size,
                "dt_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            })
            t0 = time.perf_counter()
            yield BatchResults(chunk=chunks_, statistics={"json_records_read": batch_size}, batch_size=batch_size)

    def _iter_batches(self, batch_size: int):
        if self._is_array:
            with self._open(self.json_file, binary=True) as f:
                yield from self._batch(self._iter_array(f), batch_size)
        else:
            with self._open(self.json_file) as f:
                yield from self._batch(self._iter_lines(f), batch_size)

    def _batch(self, records, batch_size: int):
        row = 0
        batch_ = []
        for record in records:
            if not isinstance(record, dict):
                raise ValueError(
                    f"JSON records must be objects, got {type(record).__name__} at row {row}"
                )
            record["_row"] = row
            row += 1
            batch_.append(record)

            if len(batch_) == batch_size:
                yield len(batch_), batch_
                batch_ = []

        if batch_:
            yield len(batch_), batch_

    def _open(self, file: Path, binary: bool = False):
        if binary:
            return gzip.open(file, "rb") if file.suffix == ".gz" else open(file, "rb")
        return gzip.open(file, "rt", encoding="utf-8-sig") if file.suffix == ".gz" \
            else open(file, "rt", encoding="utf-8-sig")

    def _starts_with_array(self, file: Path) -> bool:
        """Distinguishes a JSON array from JSON Lines by the first non-whitespace character."""
        with self._open(file) as f:
            while True:
                char = f.read(1)
                if not char or not char.isspace():
                    return char == "["

    def _iter_lines(self, file):
        for line in file:
            line = line.strip()
            if line:
                yield json.loads(line)

    def _iter_array(self, file):
        yield from ijson.items(file, "item", use_float=True)
