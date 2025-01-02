import logging
import os
from collections import namedtuple
from pathlib import Path

from neo4j import Driver, GraphDatabase, WRITE_ACCESS, SummaryCounters

QueryResult = namedtuple("query_result", ["data", "summery"])

class Neo4jContext:
    uri: str
    auth: (str, str)
    driver: Driver
    database: str

    def __init__(self, env_vars: dict):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri = env_vars["NEO4J_URI"]
        self.auth = (env_vars["NEO4J_USERNAME"],
                     env_vars["NEO4J_PASSWORD"])
        self.database = get_database_name(env_vars)
        self.__neo4j_connect()

    def query_database(self, session,  query, **kwargs) -> QueryResult:
        """
        Executes a Cypher query on the Neo4j database.
        """
        if isinstance(query, list):
            results = []
            for single_query in query:
                result = self.query_database(session, single_query, **kwargs)
                results.append(result)
            return results
        else:
            try:
                res = session.run(query, **kwargs)
                counters = res.consume().counters

                return QueryResult(res, self.__counters_2_dict(counters))

            except Exception as e:
                self.logger.error(e)
                raise e

    def __counters_2_dict(self, counters: SummaryCounters):
        return {
            "constraints_added": counters.constraints_added,
            "constraints_removed": counters.constraints_removed,
            "indexes_added": counters.indexes_added,
            "indexes_removed": counters.indexes_removed,
            "labels_added": counters.labels_added,
            "labels_removed": counters.labels_removed,
            "nodes_created": counters.nodes_created,
            "nodes_deleted": counters.nodes_deleted,
            "properties_set": counters.properties_set,
            "relationships_created": counters.relationships_created,
            "relationships_deleted": counters.relationships_deleted,
        }



    def session(self):
        return self.driver.session(database=self.database, default_access_mode=WRITE_ACCESS)

    def set_database_name(self, database_name):
        self.database = database_name

    def __neo4j_connect(self):
        self.driver = GraphDatabase.driver(uri=self.uri, auth=self.auth,
                                           notifications_min_severity="OFF")
        self.driver.verify_connectivity()
        self.logger.info(f"driver connected to instance at {self.uri}")


class ETLContext:
    neo4j: Neo4jContext
    __env_vars: dict
    path_error: Path
    path_import: Path
    path_processed: Path

    def __init__(self, env_vars: dict):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.neo4j = Neo4jContext(env_vars)
        self.__env_vars = env_vars


def get_database_name(env_vars: dict):
    """
    Returns the name of the database. If running inside pytest, the name is taken from the NEO4J_TEST_DATABASE
    environment variable. Otherwise, from NEO4J_DATABASE.
    :return:
    """
    if "PYTEST_VERSION" in env_vars:
        # we are running inside a test env...
        if "NEO4J_TEST_DATABASE" in env_vars:
            database_name = env_vars["NEO4J_TEST_DATABASE"]
        else:
            raise Exception("define NEO4J_TEST_DATABASE environment variable")
    else:
        database_name = os.getenv('NEO4J_DATABASE')
    return database_name
