import logging
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from etl_lib.ETLContext import ETLContext
from examples.gtfs.tasks.LoadAgenciesTask import LoadAgenciesTask
from examples.gtfs.tasks.LoadCalendarTask import LoadCalendarTask
from examples.gtfs.tasks.LoadRoutesTask import LoadRoutesTask
from examples.gtfs.tasks.LoadStopTimesTask import LoadStopTimesTask
from examples.gtfs.tasks.LoadStopsTask import LoadStopsTask
from examples.gtfs.tasks.LoadTripsTask import LoadTripsTask
from examples.gtfs.tasks.SchemaTask import SchemaTask

# Load environment variables from .env file
load_dotenv()


def setup_logging(log_file=None):
    """
    Set up logging to console and optionally to a log file.

    :param log_file: Path to the log file
    :type log_file: str, optional
    """
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


@click.command()
@click.argument('input_directory', type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--neo4j-uri', envvar='NEO4J_URI', help='Neo4j database URI')
@click.option('--neo4j-user', envvar='NEO4J_USERNAME', help='Neo4j username')
@click.option('--neo4j-password', envvar='NEO4J_PASSWORD', help='Neo4j password')
@click.option('--log-file', envvar='LOG_FILE', help='Path to the log file', default=None)
@click.option('--database-name', envvar='DATABASE_NAME', default='neo4j', help='Neo4j database name (default: neo4j)')
def main(input_directory, neo4j_uri, neo4j_user, neo4j_password, log_file, database_name):
    """
    Command-line tool to process files in INPUT_DIRECTORY.

    Environment variables can be configured via a .env file or overridden via CLI options:

    \b
    - NEO4J_URI: Neo4j database URI
    - NEO4J_USERNAME: Neo4j username
    - NEO4J_PASSWORD: Neo4j password
    - LOG_FILE: Path to the log file
    - DATABASE_NAME: Neo4j database name (default: neo4j)
    """
    # Set up logging
    setup_logging(log_file)
    logging.info(f"Processing directory: {input_directory}")

    # Validate Neo4j connection details
    if not neo4j_uri or not neo4j_user or not neo4j_password:
        logging.error(
            "Neo4j connection details are incomplete. Please provide NEO4J_URL, NEO4J_USER, and NEO4J_PASSWORD.")
        return

    # Log and display settings
    logging.info(f"Neo4j URL: {neo4j_uri}")
    logging.info(f"Neo4j User: {neo4j_user}")
    logging.info(f"Neo4j Database Name: {database_name}")
    if log_file:
        logging.info(f"Log File: {log_file}")

    # Example processing (replace with actual logic)
    logging.info(f"Connecting to Neo4j at {neo4j_uri} with user {neo4j_user} to access database {database_name}...")

    context = ETLContext(env_vars=dict(os.environ))

    schema = SchemaTask(context=context)
    schema.execute()

    tasks = [
        LoadAgenciesTask,
        LoadRoutesTask,
        LoadStopsTask,
        LoadTripsTask,
        LoadCalendarTask,
        LoadStopTimesTask,
        LoadTripsTask
    ]

    for task_def in tasks:
        task = task_def(context=context)
        task.execute(file=input_directory / task_def.file_name())


    logging.info("Processing complete.")


if __name__ == '__main__':
    main()
