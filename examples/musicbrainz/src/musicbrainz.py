import logging
import os
from dotenv import load_dotenv

import click
from etl_lib.cli.run_tools import cli
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import TaskGroup, ParallelTaskGroup
from etl_lib.core.utils import setup_logging
from tasks.CreateRelationshipTasks import CreateArtistAliasRelTask, CreateArtistCreditRelTask, \
    CreateArtistToAreaRelTask, CreateCreditOnTrackRelTask, CreateMediumToReleaseRelTask, CreateRecordingToWorkRelTask, \
    CreateReleaseToLabelRelTask, CreateTrackToMediumRelTask, CreateTrackToRecordingRelTask
from tasks.LoadNodesTasks import LoadAreaTask, LoadWorkTask, LoadRecordingTask, LoadLabelTask, \
    LoadReleaseTask, LoadMediumTask, LoadTrackTask, LoadArtistCreditTask, LoadArtistAliasTask, LoadArtistTask, \
    LoadArtistTypeTask, LoadAreaTypeTask, LoadWorkTypeTask, LoadMediumFormatTask

from tasks.SchemaTask import MusicBrainzSchemaTask

# Load .env variables if needed
load_dotenv()


@cli.command("import")
@click.option('--pg-host', envvar='PG_HOST', type=str, help='PostgreSQL host')
@click.option('--pg-port', envvar='PG_PORT', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-database', envvar='PG_DB', type=str, help='PostgreSQL database name')
@click.option('--pg-user', envvar='PG_USER', type=str, help='PostgreSQL username')
@click.option('--pg-password', envvar='PG_PASSWORD', type=str, help='PostgreSQL password')
@click.pass_context
def main(ctx, pg_host, pg_port, pg_database, pg_user, pg_password):
    """
    Imports MusicBrainz data from PostgreSQL to Neo4j.
    """

    log_file = ctx.obj["log_file"]
    setup_logging(log_file)
    logging.info("Starting MusicBrainz ETL pipeline")

    neo4j_uri = ctx.obj["neo4j_uri"]
    neo4j_user = ctx.obj["neo4j_user"]
    database_name = ctx.obj["database_name"]
    logging.info(f"Neo4j URL: {neo4j_uri}")
    logging.info(f"Neo4j User: {neo4j_user}")
    logging.info(f"Neo4j Database Name: {database_name}")

    sqlalchemy_uri = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"

    context = ETLContext(env_vars={**os.environ, "SQLALCHEMY_URI": sqlalchemy_uri})

    schema = MusicBrainzSchemaTask(context=context)
    schema_group = TaskGroup(context=context, tasks=[schema], name="schema-init")

    table_tasks = [
        LoadArtistTask(context=context),
        LoadArtistAliasTask(context=context),
        LoadArtistCreditTask(context=context),
        LoadTrackTask(context=context),
        LoadMediumTask(context=context),
        LoadReleaseTask(context=context),
        LoadLabelTask(context=context),
        LoadRecordingTask(context=context),
        LoadWorkTask(context=context),
        LoadAreaTask(context=context),
        LoadArtistTypeTask(context=context),
        LoadAreaTypeTask(context=context),
        LoadWorkTypeTask(context=context),
        LoadMediumFormatTask(context=context),
    ]

    table_group = ParallelTaskGroup(context=context, tasks=table_tasks, name="nodes-loading")

    relationship_tasks = [
        CreateArtistAliasRelTask(context=context),
        CreateArtistCreditRelTask(context=context),
        CreateArtistToAreaRelTask(context=context),
        CreateCreditOnTrackRelTask(context=context),
        CreateMediumToReleaseRelTask(context=context),
        CreateRecordingToWorkRelTask(context=context),
        CreateReleaseToLabelRelTask(context=context),
        CreateTrackToMediumRelTask(context=context),
        CreateTrackToRecordingRelTask(context=context),
    ]

    relationship_group = TaskGroup(context=context, tasks=relationship_tasks, name="relationship-loading")


    all_group = TaskGroup(context=context, tasks=[schema_group, table_group, relationship_group], name="musicbrainz-etl")

    context.reporter.register_tasks(all_group)

    all_group.execute()

    logging.info("MusicBrainz import completed.")

if __name__ == '__main__':
    cli()
