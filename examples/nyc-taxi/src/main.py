import logging
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from etl_lib.cli.run_tools import cli
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import TaskGroup
from etl_lib.core.utils import setup_logging

from tasks.LoadTripsParallelTask import LoadTripsParallelTask
from tasks.LoadTripsSequentialTask import LoadTripsSequentialTask
from tasks.SchemaTask import SchemaTask

# Load environment variables from .env file
load_dotenv()


@cli.command("sequential")
@click.argument("csv_file", type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.pass_context
def run_sequential(ctx, csv_file):
    """Run the ETL in sequential mode."""

    context = comon_setup(ctx, csv_file)

    pipeline = TaskGroup(context,
                         [
                             SchemaTask(context),
                             LoadTripsSequentialTask(context, csv_file)
                         ],
                         "NYC Taxi ETL (Sequential)")

    context.reporter.register_tasks(pipeline)

    pipeline.execute()
    click.echo("Sequential ETL completed.")


@cli.command("parallel")
@click.argument("csv_file", type=click.Path(exists=True))
@click.pass_context
def run_parallel(ctx, csv_file):
    """Run the ETL in parallel mode."""

    context = comon_setup(ctx, csv_file)

    pipeline = TaskGroup(context,
                         [
                             SchemaTask(context),
                             LoadTripsParallelTask(context, Path(csv_file))
                         ],
                         "NYC Taxi ETL (Parallel)")

    context.reporter.register_tasks(pipeline)

    pipeline.execute()
    click.echo("Parallel ETL completed.")


def comon_setup(ctx, csv_file):
    # Set up logging
    log_file = ctx.obj["log_file"]
    setup_logging(log_file)
    logging.info(f"Importing file: {csv_file}")
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
    return context


if __name__ == "__main__":
    cli()
