from pathlib import Path

from etl_lib.test_utils.utils import run_query
from examples.gtfs.src.tasks.LoadRoutesTask import LoadRoutesTask


def test_load_routes_task(etl_context, neo4j_driver):
    routes_txt = Path(__file__).parent / "../data/routes.txt"
    """
    Route ID 2 – Missing route_long_name, which is required.
    Route ID 3 – Invalid route_type, should be between 0-7 but is 10.
    Route ID 4 - missing agency_id
    Route ID 8 – References agency_id = 2, which does not exist in the database. But this is not a validation error
    """

    # Ensure only Agency ID 1 exists before running the test
    create_agency_cypher = """
        MERGE (a:Agency {id: '1', name: 'Transit Agency A'})
    """
    run_query(neo4j_driver, create_agency_cypher, {})

    task = LoadRoutesTask(etl_context, routes_txt)
    result = task.execute()

    assert result.success

    expected_summary = {
        'constraints_added': 0, 'constraints_removed': 0, 'csv_lines_read': 8, 'indexes_added': 0,
        'indexes_removed': 0, 'invalid_rows': 3, 'labels_added': 4, 'labels_removed': 0,
        'nodes_created': 4, 'nodes_deleted': 0, 'properties_set': 16, 'relationships_created':4,
        'relationships_deleted': 0, 'valid_rows': 5
    }
    assert result.summery == expected_summary

    cypher_query = """
        MATCH (r:Route) 
        WITH r {
            .id,
            .shortName,
            .longName,
            .type,
            agency_id: head([(a:Agency)-[:OPERATES]->(r) | a.id])
        } AS route ORDER BY r.id
        RETURN collect(route) AS routes
    """
    query_result = run_query(neo4j_driver, cypher_query, {})
    routes_from_db = query_result[0]["routes"]

    expected_routes = [
        {
            "id": "1",
            "shortName": "10",
            "longName": "Downtown Express",
            "type": 3,
            "agency_id": "1"
        },
        {
            "id": "5",
            "shortName": "75",
            "longName": "City Loop",
            "type": 1,
            "agency_id": "1"
        },
        {
            "id": "6",
            "shortName": "100",
            "longName": "Mountain Express",
            "type": 3,
            "agency_id": "1"
        },
        {
            "id": "7",
            "shortName": "200",
            "longName": "University Connector",
            "type": 2,
            "agency_id": "1"
        }
    ]

    assert routes_from_db == expected_routes

    # Verify the relationship between Agency and Route
    relationship_query = """
        MATCH (a:Agency {id: '1'})-[:OPERATES]->(r:Route)
        RETURN count(r) AS route_count
    """
    query_result = run_query(neo4j_driver, relationship_query, {})
    route_count = query_result[0]["route_count"]

    assert route_count == 4

    # Ensure route_id = 8 (invalid agency) was NOT loaded
    invalid_route_query = """
        MATCH (r:Route {id: '8'})
        RETURN count(r) AS invalid_count
    """
    query_result = run_query(neo4j_driver, invalid_route_query, {})
    invalid_count = query_result[0]["invalid_count"]

    assert invalid_count == 0
