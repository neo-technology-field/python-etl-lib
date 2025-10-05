import pytest
from etl_lib.core.ETLContext import QueryResult, append_results
from etl_lib.test_utils.utils import MockETLContext
from neo4j.exceptions import Neo4jError


def test_append_results_no_overlapping_keys():
    """Test appending two QueryResult objects with non-overlapping summary keys."""
    r1 = QueryResult(data=[1, 2], summery={'nodes_created': 10, 'relationships_created': 5})
    r2 = QueryResult(data=[3, 4], summery={'nodes_deleted': 2, 'relationships_deleted': 1})

    result = append_results(r1, r2)

    expected_data = [1, 2, 3, 4]
    expected_summery = {'nodes_created': 10, 'relationships_created': 5, 'nodes_deleted': 2, 'relationships_deleted': 1}

    assert result.data == expected_data
    assert result.summery == expected_summery


def test_append_results_with_overlapping_keys():
    """Test appending two QueryResult objects with overlapping summary keys."""
    r1 = QueryResult(data=['a', 'b'], summery={'nodes_created': 10, 'nodes_deleted': 2})
    r2 = QueryResult(data=['c', 'd'], summery={'nodes_created': 5, 'relationships_created': 3, 'nodes_deleted': 1})

    result = append_results(r1, r2)

    expected_data = ['a', 'b', 'c', 'd']
    expected_summery = {'nodes_created': 15, 'nodes_deleted': 3, 'relationships_created': 3}

    assert result.data == expected_data
    assert result.summery == expected_summery


def test_append_results_with_empty_data():
    """Test appending QueryResult objects where one has empty data/summary."""
    r1 = QueryResult(data=[], summery={'nodes_created': 5})
    r2 = QueryResult(data=[1, 2, 3], summery={'relationships_created': 2})

    result = append_results(r1, r2)

    expected_data = [1, 2, 3]
    expected_summery = {'nodes_created': 5, 'relationships_created': 2}

    assert result.data == expected_data
    assert result.summery == expected_summery


def test_append_results_with_both_empty():
    """Test appending two empty QueryResult objects."""
    r1 = QueryResult(data=[], summery={})
    r2 = QueryResult(data=[], summery={})

    result = append_results(r1, r2)

    assert result.data == []
    assert result.summery == {}


def test_query_database_single_query(etl_context: MockETLContext):
    """Test that a single Cypher query returns the expected QueryResult."""
    query = "CREATE (n:TestNode {name: 'test'}) RETURN n.name as name"

    with etl_context.neo4j.session() as session:
        result = etl_context.neo4j.query_database(session, query)

    assert isinstance(result, QueryResult)
    assert len(result.data) == 1
    assert result.data[0]["name"] == 'test'
    assert result.summery["nodes_created"] == 1
    assert "relationships_created" in result.summery


def test_query_database_list_of_queries(etl_context: MockETLContext):
    """Test that a list of Cypher queries is executed sequentially and results are appended."""
    queries = [
        "CREATE (n:Node1) RETURN n",
        "CREATE (n:Node2) RETURN n",
        "MATCH (n) WHERE n.name IS NULL DETACH DELETE n"
    ]

    with etl_context.neo4j.session() as session:
        result = etl_context.neo4j.query_database(session, queries)

    assert isinstance(result, QueryResult)
    assert len(result.data) == 2  # The last query does not return data
    assert result.summery["nodes_created"] == 2
    assert result.summery["nodes_deleted"] == 2  # The two nodes created should be deleted by the third query

    # Ensure all counts are present and have correct sums
    assert result.summery.get("relationships_created", 0) == 0
    assert result.summery.get("relationships_deleted", 0) == 0


def test_query_database_error_handling(etl_context: MockETLContext):
    """Test that Neo4jError is raised for an invalid query."""
    invalid_query = "CREATE (n:TestNode RETURN n"  # Missing closing parenthesis

    with pytest.raises(Neo4jError):
        with etl_context.neo4j.session() as session:
            etl_context.neo4j.query_database(session, invalid_query)


def test_query_database_with_parameters(etl_context: MockETLContext):
    """Test that parameters are correctly passed to the query."""
    query = "CREATE (n:TestNode {name: $name}) RETURN n.name as name"
    params = {"name": "ParametrizedNode"}

    with etl_context.neo4j.session() as session:
        result = etl_context.neo4j.query_database(session, query, **params)

    assert len(result.data) == 1
    assert result.data[0]["name"] == "ParametrizedNode"


def test_gds_context_creation(etl_context: MockETLContext):
    """Test that the GDS client is correctly created from the Neo4j context."""
    # This test is conditional on gds_available being True.
    if hasattr(etl_context.neo4j, "gds"):
        # The return value of GraphDataScience.from_neo4j_driver is a client object.
        # We can't really inspect it much here without making a call, so this is a basic check.
        assert etl_context.neo4j.gds is not None
    else:
        pytest.skip("Graph Data Science not available, skipping test")
