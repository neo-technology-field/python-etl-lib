from types import SimpleNamespace
from unittest.mock import MagicMock, call

from etl_lib.core.ETLContext import QueryResult
from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


def test_query_provider_is_evaluated_once_and_summaries_are_merged() -> None:
    neo4j = MagicMock()
    neo4j.query_database.side_effect = [
        QueryResult([], {"nodes_created": 1}),
        QueryResult([], {"nodes_created": 2}),
    ]

    class Queries(ExecuteCypherTask):
        evaluations = 0

        def _query(self) -> list[str]:
            self.evaluations += 1
            return ["RETURN $value AS first", "RETURN $value AS second"]

    task = Queries(SimpleNamespace(neo4j=neo4j))
    result = task.run_internal(value=7)
    session = neo4j.session.return_value.__enter__.return_value
    assert task.evaluations == 1
    assert result.summary == {"nodes_created": 3}
    assert neo4j.query_database.call_args_list == [
        call(session=session, query="RETURN $value AS first", value=7),
        call(session=session, query="RETURN $value AS second", value=7),
    ]
    neo4j.session.assert_called_once_with()


def test_single_query_preserves_parameter_and_summary_handling() -> None:
    neo4j = MagicMock()
    neo4j.query_database.return_value = QueryResult([], {"nodes_created": 1})

    class Query(ExecuteCypherTask):
        def _query(self) -> str:
            return "RETURN $value AS value"

    result = Query(SimpleNamespace(neo4j=neo4j)).run_internal(value=7)
    session = neo4j.session.return_value.__enter__.return_value
    neo4j.query_database.assert_called_once_with(
        session=session, query="RETURN $value AS value", value=7,
    )
    assert result.success
    assert result.summary == {"nodes_created": 1}
