import json
import time
import urllib.parse
from unittest.mock import MagicMock, call, patch

import pytest
from etl_lib.core.ETLContext import QueryResult, append_results, _fetch_oauth2_token, Neo4jContext
from etl_lib.test_utils.utils import MockETLContext
from neo4j.exceptions import Neo4jError


def test_append_results_no_overlapping_keys():
    """Test appending two QueryResult objects with non-overlapping summary keys."""
    r1 = QueryResult(data=[1, 2], summary={'nodes_created': 10, 'relationships_created': 5})
    r2 = QueryResult(data=[3, 4], summary={'nodes_deleted': 2, 'relationships_deleted': 1})

    result = append_results(r1, r2)

    expected_data = [1, 2, 3, 4]
    expected_summary = {'nodes_created': 10, 'relationships_created': 5, 'nodes_deleted': 2, 'relationships_deleted': 1}

    assert result.data == expected_data
    assert result.summary == expected_summary


def test_append_results_with_overlapping_keys():
    """Test appending two QueryResult objects with overlapping summary keys."""
    r1 = QueryResult(data=['a', 'b'], summary={'nodes_created': 10, 'nodes_deleted': 2})
    r2 = QueryResult(data=['c', 'd'], summary={'nodes_created': 5, 'relationships_created': 3, 'nodes_deleted': 1})

    result = append_results(r1, r2)

    expected_data = ['a', 'b', 'c', 'd']
    expected_summary = {'nodes_created': 15, 'nodes_deleted': 3, 'relationships_created': 3}

    assert result.data == expected_data
    assert result.summary == expected_summary


def test_append_results_with_empty_data():
    """Test appending QueryResult objects where one has empty data/summary."""
    r1 = QueryResult(data=[], summary={'nodes_created': 5})
    r2 = QueryResult(data=[1, 2, 3], summary={'relationships_created': 2})

    result = append_results(r1, r2)

    expected_data = [1, 2, 3]
    expected_summary = {'nodes_created': 5, 'relationships_created': 2}

    assert result.data == expected_data
    assert result.summary == expected_summary


def test_append_results_with_both_empty():
    """Test appending two empty QueryResult objects."""
    r1 = QueryResult(data=[], summary={})
    r2 = QueryResult(data=[], summary={})

    result = append_results(r1, r2)

    assert result.data == []
    assert result.summary == {}


def test_query_database_single_query(etl_context: MockETLContext):
    """Test that a single Cypher query returns the expected QueryResult."""
    query = "CREATE (n:TestNode {name: 'test'}) RETURN n.name as name"

    with etl_context.neo4j.session() as session:
        result = etl_context.neo4j.query_database(session, query)

    assert isinstance(result, QueryResult)
    assert len(result.data) == 1
    assert result.data[0]["name"] == 'test'
    assert result.summary["nodes_created"] == 1
    assert "relationships_created" in result.summary


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
    assert result.summary["nodes_created"] == 2
    assert result.summary["nodes_deleted"] == 2  # The two nodes created should be deleted by the third query

    # Ensure all counts are present and have correct sums
    assert result.summary.get("relationships_created", 0) == 0
    assert result.summary.get("relationships_deleted", 0) == 0


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


def _mock_urlopen(response_body: dict):
    """Return a context manager mock that yields a fake HTTP response."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(response_body).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=mock_resp)


def test_fetch_oauth2_token_sends_correct_request():
    """_fetch_oauth2_token builds the right POST body and returns parsed fields."""
    payload = {"expires_in": 3600, "access_token": "test-token-abc"}

    with patch("urllib.request.urlopen", _mock_urlopen(payload)) as mock_open:
        expires_in, token = _fetch_oauth2_token(
            "https://idp.example.com/token", "my-client", "my-secret", "openid"
        )

    assert expires_in == 3600
    assert token == "test-token-abc"

    called_req = mock_open.call_args[0][0]
    body = urllib.parse.parse_qs(called_req.data.decode())
    assert body["grant_type"] == ["client_credentials"]
    assert body["client_id"] == ["my-client"]
    assert body["client_secret"] == ["my-secret"]
    assert body["scope"] == ["openid"]


def test_fetch_oauth2_token_omits_scope_when_none():
    """_fetch_oauth2_token does not include scope when None is passed."""
    payload = {"expires_in": 1800, "access_token": "no-scope-token"}

    with patch("urllib.request.urlopen", _mock_urlopen(payload)) as mock_open:
        _fetch_oauth2_token("https://idp.example.com/token", "c", "s", None)

    body = urllib.parse.parse_qs(mock_open.call_args[0][0].data.decode())
    assert "scope" not in body


def _make_driver_mock():
    """Return a minimal GraphDatabase.driver mock that satisfies Neo4jContext.__neo4j_connect."""
    mock_driver = MagicMock()
    mock_driver.verify_connectivity.return_value = None
    return mock_driver


_BASE_ENV = {
    "NEO4J_URI": "neo4j://localhost:7687",
    "NEO4J_DATABASE": "neo4j",
}

_BASIC_ENV = {**_BASE_ENV, "NEO4J_USERNAME": "neo4j", "NEO4J_PASSWORD": "secret"}

_TOKEN_ENV = {
    **_BASE_ENV,
    "NEO4J_CLIENT_ID": "my-client",
    "NEO4J_CLIENT_SECRET": "my-secret",
    "NEO4J_TOKEN_URL": "https://idp.example.com/token",
    "NEO4J_SCOPE": "neo4j/.default",
}


def test_neo4j_context_basic_auth_uses_username_password():
    """Neo4jContext passes (username, password) to the driver when no client ID is set."""
    mock_driver = _make_driver_mock()
    with patch("etl_lib.core.ETLContext.GraphDatabase") as mock_gdb:
        mock_gdb.driver.return_value = mock_driver
        ctx = Neo4jContext(_BASIC_ENV)

    assert ctx.auth == ("neo4j", "secret")
    assert ctx._auth_manager is None
    _, kwargs = mock_gdb.driver.call_args
    assert kwargs["auth"] == ("neo4j", "secret")
    assert "auth_manager" not in kwargs


def test_neo4j_context_token_auth_uses_auth_manager():
    """Neo4jContext passes auth_manager (not auth) to the driver when NEO4J_CLIENT_ID is set."""
    mock_driver = _make_driver_mock()
    payload = {"expires_in": 3600, "access_token": "tok-xyz"}

    with patch("etl_lib.core.ETLContext.GraphDatabase") as mock_gdb, \
         patch("urllib.request.urlopen", _mock_urlopen(payload)):
        mock_gdb.driver.return_value = mock_driver
        ctx = Neo4jContext(_TOKEN_ENV)

    assert ctx.auth is None
    assert ctx._auth_manager is not None
    _, kwargs = mock_gdb.driver.call_args
    assert "auth_manager" in kwargs
    assert "auth" not in kwargs


def test_neo4j_context_token_auth_provider_calls_fetch_with_correct_args():
    """When the bearer provider is invoked, it calls _fetch_oauth2_token with the configured credentials."""
    mock_driver = _make_driver_mock()
    captured_manager = None

    def capture_driver(*args, **kwargs):
        nonlocal captured_manager
        captured_manager = kwargs.get("auth_manager")
        return mock_driver

    with patch("etl_lib.core.ETLContext.GraphDatabase") as mock_gdb, \
         patch("etl_lib.core.ETLContext._fetch_oauth2_token", return_value=(3600, "tok-abc")) as mock_fetch:
        mock_gdb.driver.side_effect = capture_driver
        Neo4jContext(_TOKEN_ENV)

        assert captured_manager is not None
        captured_manager.get_auth()

    mock_fetch.assert_called_with(
        "https://idp.example.com/token",
        "my-client",
        "my-secret",
        "neo4j/.default",
    )


def test_gds_context_creation(etl_context: MockETLContext):
    """Test that the GDS client is correctly created from the Neo4j context."""
    # This test is conditional on gds_available being True.
    if hasattr(etl_context.neo4j, "gds"):
        # The return value of GraphDataScience.from_neo4j_driver is a client object.
        # We can't really inspect it much here without making a call, so this is a basic check.
        assert etl_context.neo4j.gds is not None
    else:
        pytest.skip("Graph Data Science not available, skipping test")
