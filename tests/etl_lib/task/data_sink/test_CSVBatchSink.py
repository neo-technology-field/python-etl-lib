import csv
from pathlib import Path
from typing import Generator

import pytest

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task
from etl_lib.data_sink.CSVBatchSink import CSVBatchSink


class DummyBatchProcessor(BatchProcessor):

    def __init__(self, context: ETLContext, task: Task, batches: list[list[dict]]):
        super().__init__(context, task, None)
        self.batches = batches

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        for batch in self.batches:
            yield BatchResults(chunk=batch)


@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]


@pytest.fixture
def dummy_predecessor(etl_context, sample_data):
    batches = [sample_data[:2], sample_data[2:]]
    return DummyBatchProcessor(etl_context, Task("dummy_task"), batches)


def test_csv_batch_sink(tmp_path: Path, etl_context, dummy_predecessor, sample_data):
    file_path = tmp_path / "output.csv"
    csv_kwargs = {"quoting": csv.QUOTE_MINIMAL}
    sink = CSVBatchSink(etl_context, Task("csv_sink"), dummy_predecessor, file_path, **csv_kwargs)

    for batch_result in sink.get_batch(10):
        assert batch_result.statistics["rows_written"] == len(batch_result.chunk)

    with file_path.open(mode='r') as csvfile:
        reader = csv.reader(csvfile, **csv_kwargs)
        rows = list(reader)

    # Assert that the header exists exactly once
    assert rows[0] == ["id", "name"]
    assert rows.count(["id", "name"]) == 1

    written_data = [row for row in rows[1:]]
    expected_data = [[str(d["id"]), d["name"]] for d in sample_data]
    assert sorted(written_data) == sorted(expected_data)
