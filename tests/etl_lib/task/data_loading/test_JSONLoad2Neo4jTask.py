import gzip
import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from etl_lib.task.data_loading.JSONLoad2Neo4jTask import JSONLoad2Neo4jTask


class _Person(BaseModel):
    personId: int
    name: str


class _PersonLoadTask(JSONLoad2Neo4jTask):
    def __init__(self, context, file: Path, model=None):
        super().__init__(context, file, model=model, batch_size=2)

    def _query(self):
        return """
        UNWIND $batch AS r
        MERGE (p:Person {personId: r.personId})
            SET p.name = r.name
        """


def _write_jsonl(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def test_load_with_validation(etl_context, tmp_path):
    rows = [
        {"personId": 1, "name": "Alice"},
        {"personId": "BAD", "name": "Broken"},
        {"personId": 2, "name": "Bob"},
    ]
    json_file = tmp_path / "people.json"
    _write_jsonl(json_file, rows)

    task = _PersonLoadTask(etl_context, json_file, model=_Person)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summary["json_records_read"] == 3
    assert result.summary["valid_rows"] == 2
    assert result.summary["invalid_rows"] == 1

    error_file = Path(etl_context.env("ETL_ERROR_PATH")) / "people.error.json"
    assert error_file.exists()
    error_rows = [json.loads(line) for line in error_file.read_text().splitlines() if line]
    assert len(error_rows) == 1

    with etl_context.neo4j.session() as sess:
        records = sess.run("MATCH (p:Person) RETURN p.personId AS id, p.name AS name")
        assert {(r["id"], r["name"]) for r in records} == {(1, "Alice"), (2, "Bob")}


def test_load_without_validation_model(etl_context, tmp_path):
    json_file = tmp_path / "raw.json"
    _write_jsonl(json_file, [{"personId": 1, "name": "Alice"}, {"personId": 2, "name": "Bob"}])

    task = _PersonLoadTask(etl_context, json_file)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summary.get("valid_rows") is None
    assert result.summary.get("invalid_rows") is None
    with etl_context.neo4j.session() as sess:
        assert sess.run("MATCH (p:Person) RETURN count(p) AS c").single()["c"] == 2


def test_load_gzipped_error_file_naming(etl_context, tmp_path):
    json_file = tmp_path / "people.json.gz"
    with gzip.open(json_file, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"personId": "BAD", "name": "Broken"}) + "\n")

    task = _PersonLoadTask(etl_context, json_file, model=_Person)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summary["invalid_rows"] == 1
    assert (Path(etl_context.env("ETL_ERROR_PATH")) / "people.json.error.json").exists()


def test_load_json_array_requires_ijson(etl_context, tmp_path):
    pytest.importorskip("ijson")
    json_file = tmp_path / "people.json"
    json_file.write_text(json.dumps([{"personId": 1, "name": "Alice"}]))

    task = _PersonLoadTask(etl_context, json_file)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summary["json_records_read"] == 1


class _RowAwareLoadTask(JSONLoad2Neo4jTask):
    def __init__(self, context, file: Path, model=None):
        super().__init__(context, file, model=model, batch_size=10)

    def _query(self):
        return """
        UNWIND $batch AS r
        MERGE (p:RowAware {personId: r.personId})
            SET p.sourceRow = r._row
        """


def _source_rows(etl_context) -> list[int | None]:
    with etl_context.neo4j.session() as sess:
        records = sess.run("MATCH (p:RowAware) RETURN p.sourceRow AS row ORDER BY p.personId")
        return [r["row"] for r in records]


def test_row_field_reaches_query_only_without_model(etl_context, tmp_path):
    """Validation round-trips records through the model, which drops the injected _row field."""
    json_file = tmp_path / "rows.json"
    _write_jsonl(json_file, [{"personId": 1, "name": "Alice"}, {"personId": 2, "name": "Bob"}])

    without_model = _RowAwareLoadTask(etl_context, json_file)
    etl_context.reporter.register_tasks(without_model)
    assert without_model.execute().success is True
    assert _source_rows(etl_context) == [0, 1]

    with etl_context.neo4j.session() as sess:
        sess.run("MATCH (p:RowAware) DETACH DELETE p")

    with_model = _RowAwareLoadTask(etl_context, json_file, model=_Person)
    etl_context.reporter.register_tasks(with_model)
    assert with_model.execute().success is True
    assert _source_rows(etl_context) == [None, None]
