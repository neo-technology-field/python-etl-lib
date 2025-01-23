import logging
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from etl_lib.cli.run_tools import cli
from etl_lib.core.ETLContext import ETLContext
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


@cli.command("import")
@click.argument('input_directory', type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.pass_context
def main(ctx, input_directory):
    """
    Imports the GTFS files from the provided directory.
    """

    # Set up logging
    log_file = ctx.obj["log_file"]
    setup_logging(log_file)
    logging.info(f"Processing directory: {input_directory}")

    # Log and display settings
    neo4j_uri = ctx.obj["neo4j_uri"]
    neo4j_user = ctx.obj["neo4j_user"]
    database_name = ctx.obj["database_name"]
    logging.info(f"Neo4j URL: {neo4j_uri}")
    logging.info(f"Neo4j User: {neo4j_user}")
    logging.info(f"Neo4j Database Name: {database_name}")

    if log_file:
        logging.info(f"Log File: {log_file}")

    context = ETLContext(env_vars=dict(os.environ))

    logging.info(f"Connecting to Neo4j at {neo4j_uri} with user {neo4j_user} to access database {database_name}...")

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
    cli()
