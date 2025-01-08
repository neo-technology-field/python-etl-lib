import logging
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from etl_lib.ETLContext import ETLContext
from etl_lib.core.Task import TaskGroup
from examples.gtfs.tasks.CreateSequenceTask import CreateSequenceTask
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

    logging.info(f"Connecting to Neo4j at {neo4j_uri} with user {neo4j_user} to access database {database_name}...")

    context = ETLContext(env_vars=dict(os.environ))

    schema = SchemaTask(context=context)
    init_group = TaskGroup(context=context, tasks=[schema], name="schema-init")

    tasks = [
        LoadAgenciesTask(context=context, file=input_directory / LoadAgenciesTask.file_name()),
        LoadRoutesTask(context=context, file=input_directory / LoadRoutesTask.file_name()),
        LoadStopsTask(context=context, file=input_directory / LoadStopsTask.file_name()),
        LoadTripsTask(context=context, file=input_directory / LoadTripsTask.file_name()),
        LoadCalendarTask(context=context, file=input_directory / LoadCalendarTask.file_name()),
        LoadStopTimesTask(context=context, file=input_directory / LoadStopTimesTask.file_name()),
    ]
    csv_group = TaskGroup(context=context, tasks=tasks, name="csv-loading")

    post_group = TaskGroup(context=context, tasks=[CreateSequenceTask(context=context)], name="post-processing")

    all_group = TaskGroup(context=context, tasks=[init_group, csv_group, post_group], name="main")

    context.reporter.register_tasks(all_group)

    all_group.execute()

    logging.info("Processing complete.")


if __name__ == '__main__':
    main()
