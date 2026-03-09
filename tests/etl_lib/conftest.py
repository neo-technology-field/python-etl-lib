import os
import subprocess
import uuid
from pathlib import Path

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase, WRITE_ACCESS

from etl_lib.core.ETLContext import ETLContext
from etl_lib.test_utils.utils import get_database_name, MockETLContext, MockSQLETLContext

test_env = Path(__file__).parent / "../../.env"
load_dotenv(test_env)


def _configure_testcontainers_runtime() -> None:
    """Set Testcontainers defaults that work for Podman without affecting Docker/CI."""
    docker_host = os.getenv("DOCKER_HOST")

    if not docker_host:
        container_host = os.getenv("CONTAINER_HOST")
        if container_host:
            docker_host = container_host
            os.environ["DOCKER_HOST"] = container_host

    if not docker_host:
        # Don't override with Podman if the default Docker socket is present
        # This keeps GitHub Actions and standard Docker setups working smoothly
        if not Path("/var/run/docker.sock").exists():
            try:
                result = subprocess.run(
                    ["podman", "info", "--format", "{{.Host.RemoteSocket.Path}}"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                socket_path = result.stdout.strip()
                if result.returncode == 0 and socket_path:
                    docker_host = socket_path if socket_path.startswith("unix://") else f"unix://{socket_path}"
                    os.environ["DOCKER_HOST"] = docker_host
            except OSError:
                pass

    if docker_host and "podman" in docker_host:
        os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


_configure_testcontainers_runtime()


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
    return MockETLContext(neo4j_driver_with_empty_db, tmp_path)



@pytest.fixture(scope="session")
def postgres_container():
    """Starts a PostgreSQL TestContainer and provides a connection URL."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def sql_context(postgres_container):
    """Creates an ETLContext with an initialized SQLContext."""
    return MockSQLETLContext(postgres_container.get_connection_url())
