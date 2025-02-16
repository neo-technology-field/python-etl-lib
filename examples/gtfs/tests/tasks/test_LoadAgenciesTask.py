from pathlib import Path

from etl_lib.test_utils.utils import run_query
from examples.gtfs.src.tasks.LoadAgenciesTask import LoadAgenciesTask


def test_load_agencies_task(etl_context, neo4j_driver):
    agency_txt = Path(__file__).parent / "../data/agency.txt"
    """
    Row 3 is missing the required agency_name.
    Row 4 has an invalid email format.
    Row 7 contains an invalid URL.
    """
    task = LoadAgenciesTask(etl_context, agency_txt)
    result = task.execute()

    assert result.success

    expected_summery = {'constraints_added': 0, 'constraints_removed': 0, 'csv_lines_read': 10, 'indexes_added': 0,
                        'indexes_removed': 0, 'invalid_rows': 3, 'labels_added': 7, 'labels_removed': 0,
                        'nodes_created': 7, 'nodes_deleted': 0, 'properties_set': 56, 'relationships_created': 0,
                        'relationships_deleted': 0, 'valid_rows': 7}
    assert expected_summery == result.summery

    cypher_query = """
        MATCH (a:Agency) WHERE NOT a.id CONTAINS '-'
        WITH a {
            .id,
            .name,
            .url,
            .timezone,
            .lang,
            .phone,
            .fare_url,
            .email
        } AS agency ORDER BY a.name
        RETURN collect(agency) AS agencies
    """
    query_result = run_query(neo4j_driver, cypher_query, {})
    agencies_from_db = next(query_result).single()["agencies"]

    expected_agencies = [
        {
            "id": "1",
            "name": "Transit Agency A",
            "url": "http://www.transita.com/",
            "timezone": "America/New_York",
            "lang": "en",
            "phone": "123-456-7890",
            "fare_url": "http://www.transita.com/fares",
            "email": "contact@transita.com"
        },
        {
            "id": "2",
            "name": "Transit Agency B",
            "url": "http://www.transitb.org/",
            "timezone": "Europe/London",
            "lang": None,
            "phone": None,
            "fare_url": None,
            "email": None
        },
        {
            "id": "6",
            "name": "Transit Agency F",
            "url": "http://www.transitf.org/",
            "timezone": "Australia/Sydney",
            "lang": "en-AU",
            "phone": "987-654-3210",
            "fare_url": None,
            "email": None
        },
        {
            "id": "8",
            "name": "Transit Agency H",
            "url": "https://www.transith.com/",
            "timezone": "America/Toronto",
            "lang": None,
            "phone": None,
            "fare_url": None,
            "email": "info@transith.com"
        },
        {
            "id": "9",
            "name": "Transit Agency I",
            "url": "https://www.transiti.com/",
            "timezone": "America/Denver",
            "lang": None,
            "phone": None,
            "fare_url": "https://www.transiti.com/fares",
            "email": None
        },
        {
            "id": "10",
            "name": "Transit Agency J",
            "url": "http://www.transitj.com/",
            "timezone": "Europe/Berlin",
            "lang": "de",
            "phone": "+49 123 4567",
            "fare_url": None,
            "email": "support@transitj.com"
        }
    ]

    assert agencies_from_db == expected_agencies
