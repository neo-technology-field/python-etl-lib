import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from etl_lib.task.SchemaTask import SchemaTask
from etl_lib.test_utils.utils import MockETLContext


class _ExampleSchema(SchemaTask):
    def _schema_statements(self) -> list[str]:
        return [
            """
CREATE CONSTRAINT etl_plan_schema_id IF NOT EXISTS
FOR (n:EtlPlanSchemaRow) REQUIRE n.id IS UNIQUE
""",
            """
CREATE RANGE INDEX etl_plan_schema_name IF NOT EXISTS
FOR (n:EtlPlanSchemaRow) ON (n.name)
""",
        ]


@pytest.fixture
def schema_context(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[MockETLContext]:
    if not os.getenv("NEO4J_TEST_CONTAINER"):
        pytest.skip("Schema integration tests require a disposable Neo4j test container")
    driver = request.getfixturevalue("neo4j_driver")
    context = MockETLContext(driver, tmp_path)
    cleanup = [
        "DROP CONSTRAINT etl_plan_schema_id IF EXISTS",
        "DROP INDEX etl_plan_schema_name IF EXISTS",
    ]
    with context.neo4j.session() as session:
        for statement in cleanup:
            session.run(statement).consume()
    try:
        yield context
    finally:
        with context.neo4j.session() as session:
            for statement in cleanup:
                session.run(statement).consume()


@pytest.mark.parametrize("entry_point", ["subclass", "file"])
def test_schema_setup_is_idempotent(schema_context: MockETLContext, tmp_path: Path, entry_point: str) -> None:
    task = _ExampleSchema(schema_context)
    if entry_point == "file":
        path = tmp_path / "schema.cypher"
        path.write_text(";\n".join(task._schema_statements()), encoding="utf-8")
        task = SchemaTask.from_file(schema_context, path)

    first = task.run_internal()
    second = task.run_internal()
    assert first.success and second.success
    assert first.summary["constraints_added"] == 1
    assert second.summary["constraints_added"] == 0
    assert first.summary["indexes_added"] == 1
    assert second.summary["indexes_added"] == 0
    with schema_context.neo4j.session() as session:
        names = {record["name"] for record in session.run("SHOW CONSTRAINTS YIELD name RETURN name")}
        indexes = {record["name"] for record in session.run("SHOW INDEXES YIELD name RETURN name")}
    assert "etl_plan_schema_id" in names
    assert "etl_plan_schema_name" in indexes
