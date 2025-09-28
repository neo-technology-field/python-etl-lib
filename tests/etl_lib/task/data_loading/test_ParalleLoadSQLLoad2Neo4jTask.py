from typing import Callable

import pytest
from sqlalchemy import text

from etl_lib.core.SplittingBatchProcessor import dict_id_extractor
from etl_lib.task.data_loading.ParallelSQLLoad2Neo4jTask import ParallelSQLLoad2Neo4jTask


@pytest.fixture
def combined_context(sql_context, etl_context):
    """
    ETLContext configured with both SQL and Neo4j connections for testing.
    """
    ctx = etl_context
    ctx.sql = sql_context.sql
    return ctx


@pytest.fixture(scope="function")
def setup_rel_pairs_table(sql_context):
    """
    Create and populate rel_pairs table in Postgres.
    """
    engine = sql_context.sql.engine
    with engine.connect() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS rel_pairs
            (
                start_id
                INT,
                end_id
                INT
            );
            """
        ))
        # clear any existing
        conn.execute(text("DELETE FROM rel_pairs"))
        # insert overlapping test rows
        conn.execute(text(
            """
            INSERT INTO rel_pairs (start_id, end_id)
            SELECT i, i + 1
            FROM generate_series(1, 1000) AS g(i);
            INSERT INTO rel_pairs (start_id, end_id)
            SELECT i, i + 2
            FROM generate_series(1, 998) AS g(i);
            """
        ))
        conn.commit()
    yield
    # teardown
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE rel_pairs"))
        conn.commit()


@pytest.fixture(scope="function")
def setup_test_nodes(combined_context, setup_rel_pairs_table):
    """
    Create TestNode nodes in Neo4j for all IDs referenced by rel_pairs.
    """
    # IDs from 1 through 1001 inclusive
    ids = list(range(1, 1002))
    with combined_context.neo4j.session() as sess:
        sess.run(
            "UNWIND $ids AS id MERGE (:TestNode {id: id})",
            ids=ids
        )
    return ids


class TestRelationLoad(ParallelSQLLoad2Neo4jTask):
    def __init__(self, context):
        super().__init__(context, batch_size=2, table_size=10, max_workers=3, prefetch=2)

    def _sql_query(self) -> str:
        return 'SELECT start_id AS start, end_id AS end FROM rel_pairs;'

    def _cypher_query(self) -> str:
        return '''
            UNWIND $batch AS r
            MATCH (a:TestNode {id: r.start})
            MATCH (b:TestNode {id: r.end})
            MERGE (a)-[:RELATED_TO]->(b)
        '''

    def _id_extractor(self) -> Callable:
        return dict_id_extractor(table_size=10)


def test_parallel_load_creates_relationships(
        combined_context,
        setup_test_nodes
):
    # Compute expected relationships set
    expected = {(i, i + 1) for i in range(1, 1001)} | {(i, i + 2) for i in range(1, 999)}

    # Register and execute the task
    task = TestRelationLoad(combined_context)
    combined_context.reporter.register_tasks(task)
    result = task.execute()
    assert result.success is True

    # Verify relationships in Neo4j
    with combined_context.neo4j.session() as sess:
        records = sess.run(
            "MATCH (a:TestNode)-[r:RELATED_TO]->(b:TestNode)"
            " RETURN a.id AS a, b.id AS b"
        )
        rels = {(rec['a'], rec['b']) for rec in records}

    assert rels == expected
