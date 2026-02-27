from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from etl_lib.task.data_loading.ParquetLoad2Neo4jTask import ParquetLoad2Neo4jTask


class _PersonRow(BaseModel):
    id: int
    name: str = Field(min_length=3)


class _ParquetPersonTask(ParquetLoad2Neo4jTask):
    def _cypher_query(self) -> str:
        return """
        UNWIND $batch AS row
        CREATE (p:Person {id: row.id, name: row.name})
        """


def _write_parquet(path: Path, rows: list[dict]):
    if not rows:
        return
    # Convert list of dicts to columns
    data = {k: [row[k] for row in rows] for k in rows[0].keys()}
    table = pa.Table.from_pydict(data)
    pq.write_table(table, path)


def test_parquet_load_success(etl_context, tmp_path):
    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    parquet_file = tmp_path / "people.parquet"
    _write_parquet(parquet_file, rows)

    task = _ParquetPersonTask(
        etl_context,
        file=parquet_file,
        model=_PersonRow,
        error_file=tmp_path / "invalid.ndjson",
        batch_size=2
    )

    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summery['nodes_created'] == 3
    assert result.summery['labels_added'] == 3
    assert result.summery['properties_set'] == 6

    # Verify data in Neo4j
    with etl_context.neo4j.session() as sess:
        count = sess.run("MATCH (p:Person) RETURN count(p) as c").single()["c"]
        assert count == 3
