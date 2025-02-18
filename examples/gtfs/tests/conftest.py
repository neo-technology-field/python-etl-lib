import os
import uuid
from pathlib import Path

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase, WRITE_ACCESS

from etl_lib.core.ETLContext import ETLContext
from etl_lib.test_utils.utils import TestETLContext, get_database_name

test_env = Path(__file__).parent / "../../.env"
load_dotenv(test_env)


# DOC_INCLUDE_START_HERE
@pytest.fixture(scope="session")
def neo4j_driver():
    """
    Creates a Neo4j driver instance.
    If the environment variable NEO4J_TEST_CONTAINER is set, it will be used as the image name to start Neo4j in a
    TestContainer.
    If the variable is not set, then the following environment variables will be used to connect to running instance:
    `NEO4J_URI`
    `NEO4J_USERNAME`
    `NEO4J_PASSWORD`
    `NEO4J_TEST_DATABASE`
    The later can be used to direct tests away from the default DB,
    :return:
    """
    neo4j_container = os.getenv("NEO4J_TEST_CONTAINER")
    if neo4j_container is not None:
        print(f"found NEO4J_TEST_CONTAINER with {neo4j_container}, using test containers")
        from testcontainers.neo4j import Neo4jContainer
        with (Neo4jContainer(
                image=neo4j_container,
                username="neo4j",
                password=str(uuid.uuid4()))
                      .with_env("NEO4J_PLUGINS", "[\"apoc\", \"graph-data-science\"],")
                      .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes") as neo4j):
            driver = neo4j.get_driver()
            yield driver
            driver.close()
            return
    else:
        print(
            f"found NEO4J_TEST_CONTAINER not set, using instance at  {os.getenv('NEO4J_URI')} and database={os.getenv('NEO4J_TEST_DATABASE')}")
        # do not use test containers, but a running remote db
        with GraphDatabase.driver(os.getenv('NEO4J_URI'),
                                  auth=(os.getenv('NEO4J_USERNAME'),
                                        os.getenv('NEO4J_PASSWORD')),
                                  notifications_min_severity="OFF") as driver:
            yield driver
            driver.close()
            return


@pytest.fixture
def neo4j_driver_with_empty_db(neo4j_driver):
    with neo4j_driver.session(database=get_database_name(), default_access_mode=WRITE_ACCESS) as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield neo4j_driver


@pytest.fixture
def etl_context(neo4j_driver_with_empty_db, tmp_path) -> ETLContext:
    return TestETLContext(neo4j_driver_with_empty_db, tmp_path)

# DOC_INCLUDE_END_HERE
