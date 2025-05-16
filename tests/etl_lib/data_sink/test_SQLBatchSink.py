import pytest
from sqlalchemy import text

from etl_lib.core.BatchProcessor import BatchResults
from etl_lib.data_sink.SQLBatchSink import SQLBatchSink
from etl_lib.test_utils.utils import DummyPredecessor

@pytest.fixture(scope="function")
def setup_database(sql_context):
    engine = sql_context.sql.engine
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_table (
                i INTEGER PRIMARY KEY,
                string TEXT,
                float REAL,
                param1 TEXT,
                param2 TEXT
            )
        """))
        conn.commit()
    yield
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE test_table"))
        conn.commit()

def test_sql_batch_sink(sql_context, setup_database):
    query = """
    INSERT INTO test_table (i, string, float) VALUES (:i, :string, :float)
    """

    predecessor = DummyPredecessor([
        BatchResults(chunk=[
            {"i": 1, "string": "1", "float": 1.0},
            {"i": 2, "string": "2", "float": 2.0}
        ], statistics={}, batch_size=2)
    ])

    sut = SQLBatchSink(context=sql_context, task=None, predecessor=predecessor, query=query)
    result_gen = sut.get_batch(batch_size=2)
    statistics = [result.statistics for result in result_gen]

    assert statistics == [{'sql_rows_written': 2}]

def test_sql_batch_sink_with_empty_batch(sql_context, setup_database):
    query = """
    INSERT INTO test_table (i) VALUES (:i)
    """

    predecessor = DummyPredecessor([])

    sut = SQLBatchSink(context=sql_context, task=None, predecessor=predecessor, query=query)
    result_gen = sut.get_batch(batch_size=3)
    data = list(result_gen)

    assert data == []

def test_sql_batch_sink_with_parameters(sql_context, setup_database):
    query = """
    INSERT INTO test_table (i, param1, param2) VALUES (:i, :param1, :param2)
    """

    predecessor = DummyPredecessor([
        BatchResults(chunk=[
            {"i": 1, "param1": "foo", "param2": "bar"},
            {"i": 2, "param1": "foo", "param2": "bar"}
        ], statistics={}, batch_size=2)
    ])

    sut = SQLBatchSink(context=sql_context, task=None, predecessor=predecessor, query=query)
    result_gen = sut.get_batch(batch_size=2)
    statistics = [result.statistics for result in result_gen]

    assert statistics == [{'sql_rows_written': 2}]
