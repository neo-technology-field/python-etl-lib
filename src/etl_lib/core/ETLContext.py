import logging
from typing import Any, Dict, List, NamedTuple

from neo4j.exceptions import Neo4jError

try:
    from graphdatascience import GraphDataScience

    gds_available = False
except ImportError:
    gds_available = False
    logging.info("Graph Data Science not installed, skipping")
    GraphDataScience = None

from neo4j import GraphDatabase, Session, WRITE_ACCESS, SummaryCounters

try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine

    sqlalchemy_available = True
except ImportError:
    sqlalchemy_available = False
    logging.info("SQL Alchemy not installed, skipping")
    create_engine = None  # this and next line needed to prevent PyCharm warning
    Engine = None

from etl_lib.core.ProgressReporter import get_reporter


class QueryResult(NamedTuple):
    """Result of a query against the neo4j database."""
    data: List[Any]
    """Data as returned from the query."""
    summery: Dict[str, int]
    """Counters as reported by neo4j. Contains entries such as `nodes_created`, `nodes_deleted`, etc."""


def append_results(r1: QueryResult, r2: QueryResult) -> QueryResult:
    """
    Appends two QueryResult objects, summing the values for duplicate keys in the summary.

    Args:
        r1: The first QueryResult object.
        r2: The second QueryResult object to append.

    Returns:
        A new QueryResult object with combined data and summed summary counts.
    """
    combined_summery = r1.summery.copy()

    for key, value in r2.summery.items():
        combined_summery[key] = combined_summery.get(key, 0) + value

    return QueryResult(r1.data + r2.data, combined_summery)


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
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self.uri = env_vars["NEO4J_URI"]
        self.auth = (env_vars["NEO4J_USERNAME"],
                     env_vars["NEO4J_PASSWORD"])
        self.database = env_vars["NEO4J_DATABASE"]
        self.__neo4j_connect()

    def query_database(self, session: Session, query, **kwargs) -> QueryResult:
        """
        Executes Cypher and returns (records, counters) with retryable write semantics.
        Accepts either a single query string or a list of queries.
        Does not work with CALL {} IN TRANSACTION queries.
        """
        if isinstance(query, list):
            results = None
            for single in query:
                part = self.query_database(session, single, **kwargs)
                results = append_results(results, part) if results is not None else part
            return results

        def _tx(tx, q, params):
            res = tx.run(q, **params)
            records = list(res)
            counters = res.consume().counters
            return records, counters

        try:
            records, counters = session.execute_write(_tx, query, kwargs)
            return QueryResult(records, self.__counters_2_dict(counters))
        except Neo4jError as e:
            self.logger.error(e)
            raise

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
        Create a new Neo4j session in write mode, caller is responsible to close the session.

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

    def __neo4j_connect(self):
        self.driver = GraphDatabase.driver(uri=self.uri, auth=self.auth,
                                           notifications_min_severity="OFF")
        self.driver.verify_connectivity()
        self.logger.info(
            f"driver connected to instance at {self.uri} with username {self.auth[0]} and database {self.database}")


def gds(neo4j_context) -> GraphDataScience:
    """
    Creates a new GraphDataScience client.

    Args:
        neo4j_context: Neo4j context containing driver and database name.

    Returns:
        gds client.
    """
    return GraphDataScience.from_neo4j_driver(driver=neo4j_context.driver, database=neo4j_context.database)


if sqlalchemy_available:
    class SQLContext:
        def __init__(self, database_url: str, pool_size: int = 10, max_overflow: int = 20):
            """
            Initializes the SQL context with an SQLAlchemy engine.

            Args:
                database_url (str): SQLAlchemy connection URL.
                pool_size (int): Number of connections to maintain in the pool.
                max_overflow (int): Additional connections allowed beyond pool_size.
            """
            self.engine: Engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=1800,  # recycle connections older than 30m
                connect_args={
                    # turn on TCP keepalives on the client socket:
                    "keepalives": 1,
                    "keepalives_idle": 60,  # after 60s of idle
                    "keepalives_interval": 10,  # probe every 10s
                    "keepalives_count": 5,  # give up after 5 failed probes
                })


class ETLContext:
    """
    General context information.

    Will be passed to all :class:`~etl_lib.core.Task.Task` to provide access to environment variables and functionally
    deemed general enough that all parts of the ETL pipeline would need it.
    """

    def __init__(self, env_vars: dict):
        """
        Create a new ETLContext.

        Args:
            env_vars: Environment variables. Stored internally and can be accessed via :func:`~env` .

        The context created will contain an :class:`~Neo4jContext` and a :class:`~etl_lib.core.ProgressReporter.ProgressReporter`.
        See there for keys used from the provided `env_vars` dict.
        """
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self.neo4j = Neo4jContext(env_vars)
        self.__env_vars = env_vars
        self.reporter = get_reporter(self)
        sql_uri = self.env("SQLALCHEMY_URI")
        if sql_uri is not None and sqlalchemy_available:
            self.sql = SQLContext(sql_uri)
        if gds_available:
            self.gds = gds(self.neo4j)

    def env(self, key: str) -> Any:
        """
        Returns the value of an entry in the `env_vars` dict.

        Args:
            key: name of the entry to read.

        Returns:
            value of the entry, or None if the key is not in the dict.
        """
        if key in self.__env_vars:
            return self.__env_vars[key]
        return None
