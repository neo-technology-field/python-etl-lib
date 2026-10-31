import importlib
import json
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from etl_lib.core.ETLContext import QueryResult
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask
from etl_lib.task.data_loading.JSONLoad2Neo4jTask import JSONLoad2Neo4jTask


class _Row(BaseModel):
    value: int


class _CSVRows(CSVLoad2Neo4jTask):
    def _query(self) -> str:
        return """UNWIND $batch AS row RETURN row.value AS value"""


class _JSONRows(JSONLoad2Neo4jTask):
    def _query(self) -> str:
        return """UNWIND $batch AS row RETURN row.value AS value"""


@pytest.mark.parametrize("format_name", ["csv", "json"])
@pytest.mark.parametrize("path_kind", ["str", "path", "unset"])
def test_validation_error_directory(monkeypatch, tmp_path, format_name, path_kind):
    module = importlib.import_module("etl_lib.core.ETLContext")
    neo4j = MagicMock()
    neo4j.query_database.return_value = QueryResult([], {})
    monkeypatch.setattr(module, "Neo4jContext", lambda env: neo4j)
    monkeypatch.setattr(module, "gds_available", False)

    error_dir = tmp_path / "errors"
    error_dir.mkdir()
    settings = {}
    if path_kind != "unset":
        settings["ETL_ERROR_PATH"] = str(error_dir) if path_kind == "str" else error_dir
    context = module.ETLContext(settings)

    input_file = tmp_path / f"rows.{format_name}"
    if format_name == "csv":
        input_file.write_text("""value
1
bad
""", encoding="utf-8")
        task_type = _CSVRows
    else:
        input_file.write_text("""{"value": 1}
{"value": "bad"}
""", encoding="utf-8")
        task_type = _JSONRows

    result = task_type(context, input_file, model=_Row, batch_size=1).run_internal()
    expected_dir = tmp_path if path_kind == "unset" else error_dir
    error_file = expected_dir / "rows.error.json"
    records = [json.loads(line) for line in error_file.read_text().splitlines()]
    assert result.success
    assert result.summary["valid_rows"] == 1
    assert result.summary["invalid_rows"] == 1
    assert len(records) == 1
    assert records[0]["row"]["value"] == "bad"
    if path_kind != "unset":
        assert not (tmp_path / "rows.error.json").exists()
