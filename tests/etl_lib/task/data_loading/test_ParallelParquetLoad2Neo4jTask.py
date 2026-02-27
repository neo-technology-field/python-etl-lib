from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from etl_lib.core.SplittingBatchProcessor import dict_id_extractor
from etl_lib.task.data_loading.ParallelParquetLoad2Neo4jTask import ParallelParquetLoad2Neo4jTask


class _RelRow(BaseModel):
    start: int
    end: int


class _ParquetRelTask(ParallelParquetLoad2Neo4jTask):
    def _query(self):
        # MERGE nodes so the test needn't pre-create them
        return """
        UNWIND $batch AS r
        MERGE (a:TestNode {id: r.start})
        MERGE (b:TestNode {id: r.end})
        MERGE (a)-[:RELATED_TO]->(b)
        """

    def _id_extractor(self):
        return dict_id_extractor(table_size=self.table_size)


def _write_parquet(path: Path, rows: list[dict]):
    if not rows:
        return
    data = {k: [row[k] for row in rows] for k in rows[0].keys()}
    table = pa.Table.from_pydict(data)
    pq.write_table(table, path)


def test_parallel_parquet_load(etl_context, tmp_path):
    rows = [
        {"start": 101, "end": 202},
        {"start": 103, "end": 204},
        {"start": 205, "end": 106},
    ]
    parquet_file = tmp_path / "rels.parquet"
    _write_parquet(parquet_file, rows)

    task = _ParquetRelTask(
        etl_context,
        file=parquet_file,
        model=_RelRow,
        error_file=tmp_path / "invalid.ndjson",
        table_size=10,
        batch_size=2,
        max_workers=4,
        prefetch=2
    )

    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    assert result.summery['relationships_created'] == 3
    assert result.summery['nodes_created'] == 6

    # Sanity check
    with etl_context.neo4j.session() as sess:
        count = sess.run("MATCH ()-[:RELATED_TO]->() RETURN count(*) as c").single()["c"]
        assert count == 3
