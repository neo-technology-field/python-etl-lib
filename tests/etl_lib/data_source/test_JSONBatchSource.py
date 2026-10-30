import gzip
import json
from pathlib import Path

import pytest

from etl_lib.core.BatchProcessor import BatchResults
from etl_lib.data_source.JSONBatchSource import JSONBatchSource
from etl_lib.test_utils.utils import DummyContext

RECORDS = [
    {"string": "Hello, World!", "integer": 42, "float": 3.14},
    {"string": "Ümläuts & Accénts", "integer": -1, "float": -0.01},
    {"string": "Escape \tTab", "integer": 999999, "float": 1000000.0},
]


def write_json_lines(path: Path, records: list[dict], gzipped: bool = False) -> Path:
    opener = gzip.open if gzipped else open
    with opener(path, mode="wt", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    return path


def write_json_array(path: Path, records: list[dict], gzipped: bool = False) -> Path:
    opener = gzip.open if gzipped else open
    with opener(path, mode="wt", encoding="utf-8") as f:
        json.dump(records, f)
    return path


def expected_chunk(records: list[dict]) -> list[dict]:
    return [{**record, "_row": idx} for idx, record in enumerate(records)]


@pytest.mark.parametrize("gzipped", [False, True], ids=["plain", "gzip"])
def test_reads_json_lines(tmp_path: Path, gzipped):
    suffix = ".jsonl.gz" if gzipped else ".jsonl"
    json_file = write_json_lines(tmp_path / f"records{suffix}", RECORDS, gzipped=gzipped)

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)
    batches = list(processor.get_batch(len(RECORDS)))

    assert len(batches) == 1, "Expected a single batch"

    batch: BatchResults = batches[0]
    assert batch.chunk == expected_chunk(RECORDS)
    assert batch.statistics == {"json_records_read": len(RECORDS)}
    assert batch.batch_size == len(RECORDS)


@pytest.mark.parametrize("gzipped", [False, True], ids=["plain", "gzip"])
def test_reads_json_array(tmp_path: Path, gzipped):
    suffix = ".json.gz" if gzipped else ".json"
    json_file = write_json_array(tmp_path / f"records{suffix}", RECORDS, gzipped=gzipped)

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)
    batches = list(processor.get_batch(len(RECORDS)))

    assert len(batches) == 1, "Expected a single batch"

    batch: BatchResults = batches[0]
    assert batch.chunk == expected_chunk(RECORDS)
    assert batch.statistics == {"json_records_read": len(RECORDS)}
    assert batch.batch_size == len(RECORDS)


@pytest.mark.parametrize("batch_size", [2, 5])
def test_batching(tmp_path: Path, batch_size):
    total_rows = 10
    records = [{"id": i, "name": f"Row {i}"} for i in range(total_rows)]
    json_file = write_json_lines(tmp_path / "large.jsonl", records)

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)
    batches = list(processor.get_batch(batch_size))

    expected_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    assert len(batches) == expected_batches

    all_rows = [row for batch in batches for row in batch.chunk]
    assert len(all_rows) == total_rows
    assert [row["_row"] for row in all_rows] == list(range(total_rows))


def test_skips_blank_lines(tmp_path: Path):
    json_file = tmp_path / "sparse.jsonl"
    json_file.write_text('\n{"a": 1}\n\n\n{"a": 2}\n\n', encoding="utf-8")

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)
    batches = list(processor.get_batch(10))

    assert [row["a"] for row in batches[0].chunk] == [1, 2]
    assert [row["_row"] for row in batches[0].chunk] == [0, 1]


def test_empty_file_yields_no_batches(tmp_path: Path):
    json_file = tmp_path / "empty.jsonl"
    json_file.write_text("", encoding="utf-8")

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)

    assert list(processor.get_batch(10)) == []


def test_rejects_non_object_records(tmp_path: Path):
    json_file = tmp_path / "scalars.jsonl"
    json_file.write_text("[1, 2, 3]\n", encoding="utf-8")

    processor = JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)

    with pytest.raises(ValueError, match="must be objects"):
        list(processor.get_batch(10))


def test_json_array_requires_ijson(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("etl_lib.data_source.JSONBatchSource.ijson", None)
    json_file = write_json_array(tmp_path / "records.json", RECORDS)

    with pytest.raises(ImportError, match="ijson"):
        JSONBatchSource(context=DummyContext(), task=None, json_file=json_file)
