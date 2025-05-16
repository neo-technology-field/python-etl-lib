from etl_lib.core.BatchProcessor import BatchResults
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.test_utils.utils import DummyPredecessor


def test_cypher_batch_sink(etl_context):
    query = """
    UNWIND $batch AS row
    CREATE (n:TestNode {i: row.i, string: row.string, float: row.float})
    RETURN count(n) AS count
    """

    predecessor = DummyPredecessor([
        BatchResults(chunk=[
            {"i": 1, "string": "1", "float": 1.0},
            {"i": 2, "string": "2", "float": 2.0},
            {"i": 3, "string": "3", "float": 3.0}
        ], statistics={}, batch_size=3),
        BatchResults(chunk=[
            {"i": 4, "string": "4", "float": 4.0},
            {"i": 5, "string": "5", "float": 5.0}
        ], statistics={}, batch_size=2)
    ])

    sut = CypherBatchSink(context=etl_context, task=None, predecessor=predecessor, query=query)
    result_gen = sut.get_batch(batch_size=3)
    statistics = [result.statistics for result in result_gen]

    assert statistics == [{'constraints_added': 0,
                           'constraints_removed': 0,
                           'indexes_added': 0,
                           'indexes_removed': 0,
                           'labels_added': 3,
                           'labels_removed': 0,
                           'nodes_created': 3,
                           'nodes_deleted': 0,
                           'properties_set': 9,
                           'relationships_created': 0,
                           'relationships_deleted': 0},
                          {'constraints_added': 0,
                           'constraints_removed': 0,
                           'indexes_added': 0,
                           'indexes_removed': 0,
                           'labels_added': 2,
                           'labels_removed': 0,
                           'nodes_created': 2,
                           'nodes_deleted': 0,
                           'properties_set': 6,
                           'relationships_created': 0,
                           'relationships_deleted': 0}
                          ]


def test_cypher_batch_sink_with_empty_batch(etl_context):
    query = """
    UNWIND $batch AS row
    CREATE (n:TestNode {i: row.i})
    RETURN count(n) AS count
    """

    predecessor = DummyPredecessor([])

    sut = CypherBatchSink(context=etl_context, task=None, predecessor=predecessor, query=query)
    result_gen = sut.get_batch(batch_size=3)
    data = list(result_gen)

    assert data == []  # No data should be returned


def test_cypher_batch_sink_with_parameters(etl_context):
    query = """
    UNWIND $batch AS row
    CREATE (n:TestNode {i: row.i, param1: $param1, param2: $param2})
    RETURN count(n) AS count
    """

    predecessor = DummyPredecessor([
        BatchResults(chunk=[
            {"i": 1},
            {"i": 2}
        ], statistics={}, batch_size=2)
    ])

    sut = CypherBatchSink(context=etl_context, task=None, predecessor=predecessor, query=query, param1="param1",
                          param2="param2")
    result_gen = sut.get_batch(batch_size=2)
    data = [result.statistics for result in result_gen]

    assert data == [{'constraints_added': 0,
                     'constraints_removed': 0,
                     'indexes_added': 0,
                     'indexes_removed': 0,
                     'labels_added': 2,
                     'labels_removed': 0,
                     'nodes_created': 2,
                     'nodes_deleted': 0,
                     'properties_set': 6,
                     'relationships_created': 0,
                     'relationships_deleted': 0}]
