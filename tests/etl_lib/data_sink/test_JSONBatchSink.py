import json
from pathlib import Path
from typing import Generator

import pytest

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.Task import Task
from etl_lib.data_sink.JSONBatchSink import JSONBatchSink
from etl_lib.data_source.JSONBatchSource import JSONBatchSource
from etl_lib.test_utils.utils import DummyContext

RECORDS = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"},
]


class DummyBatchProcessor(BatchProcessor):

    def __init__(self, context, task: Task, batches: list[list[dict]]):
        super().__init__(context, task, None)
        self.batches = batches

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        for batch in self.batches:
            yield BatchResults(chunk=batch)


@pytest.fixture
def predecessor():
    batches = [RECORDS[:2], RECORDS[2:]]
    return DummyBatchProcessor(DummyContext(), Task("dummy_task"), batches)


def test_writes_json_lines(tmp_path: Path, predecessor):
    file_path = tmp_path / "output.jsonl"
    sink = JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, file_path)

    for batch_result in sink.get_batch(10):
        assert batch_result.statistics["rows_written"] == len(batch_result.chunk)

    lines = file_path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == RECORDS


def test_appends_to_existing_file(tmp_path: Path, predecessor):
    file_path = tmp_path / "existing.jsonl"
    file_path.write_text(json.dumps({"id": 0, "name": "Zoe"}) + "\n", encoding="utf-8")
    sink = JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, file_path)

    list(sink.get_batch(10))

    lines = file_path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == [0, 1, 2, 3]


def test_writes_json_array(tmp_path: Path, predecessor):
    file_path = tmp_path / "output.json"
    sink = JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, file_path, format="array")

    list(sink.get_batch(10))

    assert json.loads(file_path.read_text(encoding="utf-8")) == RECORDS


def test_array_format_overwrites_existing_file(tmp_path: Path, predecessor):
    file_path = tmp_path / "existing.json"
    file_path.write_text('[{"id": 0, "name": "Zoe"}]', encoding="utf-8")
    sink = JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, file_path, format="array")

    list(sink.get_batch(10))

    assert json.loads(file_path.read_text(encoding="utf-8")) == RECORDS


def test_rejects_unknown_format(tmp_path: Path, predecessor):
    with pytest.raises(ValueError, match="format"):
        JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, tmp_path / "out.json", format="xml")


def test_round_trip(tmp_path: Path, predecessor):
    file_path = tmp_path / "round_trip.jsonl"
    sink = JSONBatchSink(DummyContext(), Task("json_sink"), predecessor, file_path)
    list(sink.get_batch(10))

    source = JSONBatchSource(context=DummyContext(), task=None, json_file=file_path)
    rows = [row for batch in source.get_batch(10) for row in batch.chunk]

    assert [{k: v for k, v in row.items() if k != "_row"} for row in rows] == RECORDS
