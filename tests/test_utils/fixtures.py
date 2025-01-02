import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase, WRITE_ACCESS

from etl_lib.ETLContext import ETLContext

test_env = Path(__file__).parent / "../../.env"
load_dotenv(test_env)


@pytest.fixture
def neo4j_driver():
    with GraphDatabase.driver(os.getenv('NEO4J_URI'),
                              auth=(os.getenv('NEO4J_USERNAME'),
                                    os.getenv('NEO4J_PASSWORD')),
                              notifications_min_severity="OFF") as driver:
        yield driver


@pytest.fixture
def neo4j_driver_with_empty_db(neo4j_driver):
    with neo4j_driver.session(database="neo4j", default_access_mode=WRITE_ACCESS) as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield neo4j_driver


@pytest.fixture
def etl_context() -> ETLContext:
    etl_context = ETLContext(env_vars=dict(os.environ))
    with etl_context.neo4j.session() as session:
        etl_context.neo4j.query_database(session, "MATCH (n) DETACH DELETE n")
    return etl_context
