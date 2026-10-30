import json
from pathlib import Path

from pydantic import BaseModel

from etl_lib.core.SplittingBatchProcessor import dict_id_extractor
from etl_lib.task.data_loading.ParallelJSONLoad2Neo4jTask import \
    ParallelJSONLoad2Neo4jTask


class _RelRow(BaseModel):
    start: int
    end: int


class _JSONRelTask(ParallelJSONLoad2Neo4jTask):
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


def _write_jsonl(path: Path, rows: list[dict[str, int]]):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _run_task(etl_context, tmp_path, rows, model, error_file):
    json_file = tmp_path / "rels.json"
    _write_jsonl(json_file, rows)
    task = _JSONRelTask(etl_context,
                        file=json_file,
                        model=model,
                        error_file=error_file,
                        table_size=10,
                        batch_size=2,
                        max_workers=4,
                        prefetch=2)
    etl_context.reporter.register_tasks(task)
    return task.execute(), rows


def test_parallel_json_success(etl_context, tmp_path):
    rows = [{"start": 101, "end": 202}, {"start": 103, "end": 204}, {"start": 205, "end": 106}]
    result, rows = _run_task(etl_context, tmp_path, rows, model=None, error_file=None)

    assert result.success is True
    assert result.summary["relationships_created"] == 3

    with etl_context.neo4j.session() as sess:
        records = sess.run("MATCH (a:TestNode)-[:RELATED_TO]->(b:TestNode) RETURN a.id AS a, b.id AS b")
        rels = {(rec["a"], rec["b"]) for rec in records}
    assert rels.issuperset({(r["start"], r["end"]) for r in rows})


def test_parallel_json_with_validation(etl_context, tmp_path):
    rows = [
        {"start": 101, "end": 202},
        {"start": "BAD", "end": 106},
        {"start": 205, "end": 106},
    ]
    result, _ = _run_task(etl_context, tmp_path, rows, model=_RelRow,
                          error_file=tmp_path / "invalid.ndjson")

    assert result.success is True
    assert result.summary["valid_rows"] == 2
    assert result.summary["invalid_rows"] == 1
    assert (tmp_path / "invalid.ndjson").exists()
