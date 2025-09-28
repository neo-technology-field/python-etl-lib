from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.task.data_loading.ParallelCSVLoad2Neo4jTask import ParallelCSVLoad2Neo4jTask
from etl_lib.core.SplittingBatchProcessor import dict_id_extractor


class _RelRow(BaseModel):
    start: int
    end: int


class _CSVRelTask(ParallelCSVLoad2Neo4jTask):
    def __init__(self,
                 context,
                 *,
                 file: Path,
                 model: Type[BaseModel] | None,
                 error_file: Path | None,
                 table_size: int,
                 batch_size: int,
                 max_workers: int,
                 prefetch: int,
                 **csv_reader_kwargs):
        super().__init__(context,
                         file=file,
                         model=model,
                         error_file=error_file,
                         table_size=table_size,
                         batch_size=batch_size,
                         max_workers=max_workers,
                         prefetch=prefetch,
                         **csv_reader_kwargs)

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


def _write_csv(path: Path, rows: list[dict[str, int]]):
    import csv
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["start", "end"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_parallel_csv_success(etl_context, tmp_path):
    rows = [
        {"start": 101, "end": 202},
        {"start": 103, "end": 204},
        {"start": 205, "end": 106},
    ]
    csv_file = tmp_path / "rels.csv"
    _write_csv(csv_file, rows)

    task = _CSVRelTask(etl_context,
                       file=csv_file,
                       model=_RelRow,                       # use model so CSV strings → ints
                       error_file=tmp_path / "invalid.ndjson",
                       table_size=10,
                       batch_size=2,
                       max_workers=4,
                       prefetch=2)

    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    # Neo4j summary counters from MERGE operations
    assert result.summery['labels_added'] == 6
    assert result.summery['properties_set'] == 6
    assert result.summery['relationships_created'] == 3
    assert result.summery['nodes_created'] == 6

    # sanity check: relationships present
    with etl_context.neo4j.session() as sess:
        records = sess.run(
            "MATCH (a:TestNode)-[:RELATED_TO]->(b:TestNode) "
            "RETURN a.id AS a, b.id AS b"
        )
        rels = {(rec["a"], rec["b"]) for rec in records}
    expected = {(r["start"], r["end"]) for r in rows}
    assert rels.issuperset(expected)


def test_parallel_csv_with_validation(etl_context, tmp_path):
    rows = [
        {"start": 101, "end": 202},
        {"start": 103, "end": 204},
        {"start": "BAD", "end": 106},   # invalid by model; should be counted as invalid and skipped
        {"start": 205, "end": 106},
    ]
    valid = [rows[0], rows[1], rows[3]]

    csv_file = tmp_path / "rels.csv"
    _write_csv(csv_file, rows)

    task = _CSVRelTask(etl_context,
                       file=csv_file,
                       model=_RelRow,                       # triggers ValidationBatchProcessor internally
                       error_file=tmp_path / "invalid.ndjson",
                       table_size=10,
                       batch_size=2,
                       max_workers=4,
                       prefetch=2)

    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result.success is True
    # ValidationBatchProcessor contributes to merged summary
    assert result.summery.get("valid_rows") == 3
    assert result.summery.get("invalid_rows") == 1

    with etl_context.neo4j.session() as sess:
        records = sess.run(
            "MATCH (a:TestNode)-[:RELATED_TO]->(b:TestNode) "
            "RETURN a.id AS a, b.id AS b"
        )
        rels = {(rec["a"], rec["b"]) for rec in records}

    expected = {(int(r["start"]), int(r["end"])) for r in valid}
    assert rels.issuperset(expected)
