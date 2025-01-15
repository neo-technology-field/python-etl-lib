import os
from pathlib import Path
from typing import Any

from anytree import Node
from neo4j.time import Date

from etl_lib.core.ETLContext import QueryResult
from etl_lib.core.Task import Task


def get_test_file(file_name):
    """
    Returns the absolut path of a file that exists in `tests/data/` directory.
    """
    return Path(__file__).parent / f"../data/{file_name}"


def run_write_query(driver, query, data):
    with driver.session(database=get_database_name()) as session:
        session.run(query, data=data)


def get_node_count(driver, label: str) -> int:
    """Get the count of nodes with the specified label"""
    query = f"MATCH (n:{label}) RETURN COUNT(n) AS count"
    with driver.session(database=get_database_name()) as session:
        result = session.run(query)
        return result.single()["count"]


def get_relationship_count(driver, rel_type: str) -> int:
    """Get the count of relationships with the specified type"""
    query = f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) AS count"
    with driver.session(database=get_database_name()) as session:
        result = session.run(query)
        return result.single()["count"]


def get_graph(driver):
    """
    Return a grap representation of all data in the database.
    The returned structure is an array of dicts. Each dict has the following keys:
    `start`, `end`, and `rel` representing each relationship found in the graph.
    Use the following query to generate this structure from a known good graph:

        MATCH (s)-[r]->(e)
        WITH {labels:labels(s), props:properties(s)} AS start, {type:type(r), props:properties(r)} AS rel, {labels:labels(e), props:properties(e)} AS end
        RETURN {start:start, rel:rel, end:end}
    """
    with  driver.session(database=get_database_name()) as session:
        records = session.run(
            """
            MATCH (s)-[r]->(e)
            WITH {labels:labels(s), props:properties(s)} AS start, 
                {type:type(r), props:properties(r)} AS rel, 
                {labels:labels(e), props:properties(e)} AS end 
            RETURN {start:start, rel:rel, end:end} AS graph
            """
        )
        data = [record.data()["graph"] for record in records]
        return convert_neo4j_date_to_string(data, "%Y-%m-%d")


def convert_neo4j_date_to_string(data, date_format):
    """
    Recursively converts all neo4j.time.Date instances in a dictionary into strings using the provided format.

    :param data: The input dictionary or list to process.
    :param date_format: A format string compatible with Python's strftime.
    :return: The processed dictionary or list with dates converted to strings.
    """
    if isinstance(data, dict):
        return {key: convert_neo4j_date_to_string(value, date_format) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_neo4j_date_to_string(item, date_format) for item in data]
    elif isinstance(data, Date):
        return data.to_native().strftime(date_format)
    else:
        return data



def get_database_name():
    if os.getenv("NEO4J_TEST_CONTAINER") is None:
        # not running with test containers. expect test db to be set
        if os.getenv("NEO4J_TEST_DATABASE") is not None:
            return os.getenv("NEO4J_TEST_DATABASE")
        else:
            raise Exception("define NEO4J_TEST_DATABASE environment variable")


class DummyReporter:

    def register_tasks(self, main: Task):
        pass

    def started_task(self, task: Task) -> Node:
        pass

    def finished_task(self, success: bool, summery: dict, error: str = None) -> Node:
        pass

    def report_progress(self, batches: int, expected_batches: int, stats: dict) -> None:
        pass


class DummyNeo4jContext:

    def query_database(self, session, query, **kwargs) -> QueryResult:
        return QueryResult([], {})

    def session(self, database=None):
        return None


class DummyContext:
    neo4j: DummyNeo4jContext
    __env_vars: dict
    path_error: Path
    path_import: Path
    path_processed: Path
    reporter = DummyReporter()

    def env(self, key: str) -> Any:
        pass
