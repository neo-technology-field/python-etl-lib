from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from etl_lib.task.SchemaTask import SchemaTask, _split_statements


def test_schema_subclass_supplies_statement_list() -> None:
    class ExampleSchema(SchemaTask):
        def _schema_statements(self) -> list[str]:
            return ["RETURN 1 AS first", "RETURN 2 AS second"]

    assert ExampleSchema(None)._query() == ["RETURN 1 AS first", "RETURN 2 AS second"]


def test_schema_file_is_read_when_execution_requests_statements(tmp_path: Path) -> None:
    path = tmp_path / "schema.cypher"
    task = SchemaTask.from_file(None, path)
    path.write_text(
        """// Schema setup
RETURN 1 AS first;
RETURN 2 AS second;
""",
        encoding="utf-8",
    )
    assert task._query() == ["RETURN 1 AS first", "RETURN 2 AS second"]


def test_missing_schema_file_raises_when_read(tmp_path: Path) -> None:
    task = SchemaTask.from_file(None, tmp_path / "missing.cypher")
    with pytest.raises(FileNotFoundError):
        task._query()


@pytest.mark.parametrize("text", ["", "  ; ;  ", "// comment only", "/* comment only */"])
def test_empty_schema_is_a_noop(tmp_path: Path, text: str) -> None:
    path = tmp_path / "schema.cypher"
    path.write_text(text, encoding="utf-8")
    neo4j = MagicMock()
    result = SchemaTask.from_file(SimpleNamespace(neo4j=neo4j), path).run_internal()
    assert result.success
    assert result.summary == {}
    neo4j.query_database.assert_not_called()


def test_statement_boundaries_respect_quotes_identifiers_and_comments() -> None:
    text = """// ignored ; delimiter
RETURN 'value;one' AS first;
/* ignored ; delimiter */ RETURN "value;two" AS second;
RETURN 1 AS `column;name`;
"""
    assert _split_statements(text) == [
        "RETURN 'value;one' AS first",
        'RETURN "value;two" AS second',
        "RETURN 1 AS `column;name`",
    ]


def test_escaped_quote_and_doubled_backtick_preserve_boundaries() -> None:
    text = r"""RETURN 'it\'s;fine' AS value; RETURN 1 AS `a``;b`;"""
    assert _split_statements(text) == [
        r"""RETURN 'it\'s;fine' AS value""",
        "RETURN 1 AS `a``;b`",
    ]


@pytest.mark.parametrize("text", ["RETURN 'unfinished", "RETURN 1 AS `unfinished", "/* unfinished"])
def test_unterminated_schema_tokens_fail_explicitly(text: str) -> None:
    with pytest.raises(ValueError, match="Unterminated"):
        _split_statements(text)
