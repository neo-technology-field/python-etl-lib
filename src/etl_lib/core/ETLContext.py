import logging
from typing import NamedTuple, Any

from graphdatascience import GraphDataScience
from neo4j import GraphDatabase, WRITE_ACCESS, SummaryCounters

from etl_lib.core.ProgressReporter import get_reporter


class QueryResult(NamedTuple):
    """Result of a query against the neo4j database."""
    data: []
    """Data as returned from the query."""
    summery: {}
    """Counters as reported by neo4j. Contains entries such as `nodes_created`, `nodes_deleted`, etc."""


def append_results(r1: QueryResult, r2: QueryResult) -> QueryResult:
    return QueryResult(r1.data + r2.data, r1.summery + r2.summery)


class Neo4jContext:
    """
    Holds the connection to the neo4j database and provides facilities to execute queries.
    """

    def __init__(self, env_vars: dict):
        """
        Create a new Neo4j context.

        Reads the following env_vars keys:
        - `NEO4J_URI`,
        - `NEO4J_USERNAME`,
        - `NEO4J_PASSWORD`.
        - `NEO4J_DATABASE`,
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri = env_vars["NEO4J_URI"]
        self.auth = (env_vars["NEO4J_USERNAME"],
                     env_vars["NEO4J_PASSWORD"])
        self.database = env_vars["NEO4J_DATABASE"]
        self.__neo4j_connect()

    def query_database(self, session, query, **kwargs) -> QueryResult:
        """
        Executes a Cypher query on the Neo4j database.

        Args:
            session: Neo4j database session.
            query: Cypher query either as a single query or as a list.
        """
        if isinstance(query, list):
            results = []
            for single_query in query:
                result = self.query_database(session, single_query, **kwargs)
                results = append_results(results, result)
            return results
        else:
            try:
                res = session.run(query, **kwargs)
                counters = res.consume().counters

                return QueryResult(res, self.__counters_2_dict(counters))

            except Exception as e:
                self.logger.error(e)
                raise e

    @staticmethod
    def __counters_2_dict(counters: SummaryCounters):
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

    def session(self, database=None):
        """
        Create a new Neo4j session in write mode.

        Caller is responsible to close the session.
        Args:
            database: name of the database to use for this session. If not provided, the database name provided during
                construction will be used.

        Returns:
            newly created Neo4j session.

        """
        if database is None:
            return self.driver.session(database=self.database, default_access_mode=WRITE_ACCESS)
        else:
            return self.driver.session(database=database, default_access_mode=WRITE_ACCESS)

    def gds(self, database=None) -> GraphDataScience:
        """
        Creates a new GraphDataScience client.

        Args:
            database: Name of the database to use for this dgs client.
                If not provided, the database name provided during construction will be used.

        Returns:
            gds client.
        """
        if database is None:
            return GraphDataScience.from_neo4j_driver(driver=self.driver, database=self.database)
        else:
            return GraphDataScience.from_neo4j_driver(driver=self.driver, database=database)

    def __neo4j_connect(self):
        self.driver = GraphDatabase.driver(uri=self.uri, auth=self.auth,
                                           notifications_min_severity="OFF")
        self.driver.verify_connectivity()
        self.logger.info(
            f"driver connected to instance at {self.uri} with username {self.auth[0]} and database {self.database}")


class ETLContext:
    """
    General context information.

    Will be passed to all :py:class:`etl_lib.core.Task` to provide access to environment variables and functionally
    deemed general enough that all parts of the ETL pipeline would need it.
    """
    neo4j: Neo4jContext
    __env_vars: dict

    def __init__(self, env_vars: dict):
        """
        Create a new ETLContext.

        Args:
            env_vars: Environment variables. Stored internally and can be accessed via :py:func:`~env` .

        The context created will contain an :py:class:`~Neo4jContext` and a :py:class:`ProgressReporter`.
        See there for keys used from the provided `env_vars` dict.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.neo4j = Neo4jContext(env_vars)
        self.__env_vars = env_vars
        self.reporter = get_reporter(self)

    def env(self, key: str) -> Any:
        """
        Returns the value of an entry in the `env_vars` dict.

        Args:
            key: name of the entry to read.

        Returns:
            va  lue of the entry, or None if the key is not in the dict.
        """
        if key in self.__env_vars:
            return self.__env_vars[key]
