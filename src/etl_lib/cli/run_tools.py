from datetime import datetime

import click
import neo4j
from neo4j import GraphDatabase
from neo4j.time import DateTime
from tabulate import tabulate


def __convert_date_time(input_date_time) -> datetime | None:
    if input_date_time is None:
        return None
    return input_date_time.to_native().strftime("%Y-%m-%d %H:%M")


def __duration_from_start_end(start_time: DateTime | None, end_time: DateTime | None) -> str | None:
    if start_time is None or end_time is None:
        return None

    # Convert neo4j.time.DateTime to native Python datetime
    start_time = start_time.to_native()
    end_time = end_time.to_native()

    # Calculate the duration as a timedelta
    duration = end_time - start_time

    # Extract hours, minutes, and seconds
    total_seconds = int(duration.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # Format as HH:MM:SS
    return f"{hours}:{minutes:02}:{seconds:02}"


def __print_details(driver, run_id):
    records, _, _ = driver.execute_query("""
    MATCH (:ETLRun {uuid : $id})-[:HAS_SUB_TASK*]->(task)-[:HAS_STATS]->(stats)
    WITH task, stats ORDER BY task.order ASC
    RETURN task.task AS task, task.status AS status, properties(stats) AS stats
    """, id=run_id, routing_=neo4j.RoutingControl.READ)

    print("Showing detailed stats for each task. Task without non-zero stats are omitted.")
    for record in records:
        rows = [(key, value) for key, value in record["stats"].items() if value != 0]
        if rows:
            print(f"Showing statistics for Task '{record['task']}' with status '{record['status']}'")
            print(tabulate(rows, headers=["Name", "Value"], tablefmt='psql'))


def __driver(ctx):
    neo4j_uri = ctx.obj["neo4j_uri"]
    neo4j_user = ctx.obj["neo4j_user"]
    database_name = ctx.obj["database_name"]
    neo4j_password = ctx.obj["neo4j_password"]
    return GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password), database=database_name,
                                notifications_min_severity="OFF", user_agent="ETL CLI")


@click.group()
@click.option('--neo4j-uri', envvar='NEO4J_URI', help='Neo4j database URI')
@click.option('--neo4j-user', envvar='NEO4J_USERNAME', help='Neo4j username')
@click.option('--neo4j-password', envvar='NEO4J_PASSWORD', help='Neo4j password')
@click.option('--log-file', envvar='LOG_FILE', help='Path to the log file', default=None)
@click.option('--database-name', envvar='DATABASE_NAME', default='neo4j', help='Neo4j database name (default: neo4j)')
@click.pass_context
def cli(ctx, neo4j_uri, neo4j_user, neo4j_password, log_file, database_name):
    """
        Command-line tool for ETL pipelines.

        Environment variables can be configured via a .env file or overridden via CLI options:

        \b
        - NEO4J_URI: Neo4j database URI
        - NEO4J_USERNAME: Neo4j username
        - NEO4J_PASSWORD: Neo4j password
        - LOG_FILE: Path to the log file
        - DATABASE_NAME: Neo4j database name (default: neo4j)
        """

    # Validate Neo4j connection details
    if not neo4j_uri or not neo4j_user or not neo4j_password:
        print(
            "Neo4j connection details are incomplete. Please provide NEO4J_URL, NEO4J_USER, and NEO4J_PASSWORD.")
        return

    ctx.ensure_object(dict)
    ctx.obj['neo4j_uri'] = neo4j_uri
    ctx.obj['neo4j_user'] = neo4j_user
    ctx.obj['neo4j_password'] = neo4j_password
    ctx.obj['database_name'] = database_name
    ctx.obj['log_file'] = log_file


@cli.command()
@click.option("--number-runs", default=10, help="Number of rows to process, defaults to 10", type=int)
@click.pass_context
def query(ctx, number_runs):
    """
    Retrieve the list of the last x ETL runs from the database and display them.
    """
    print(f"Listing runs in database '{ctx.obj['database_name']}'")
    with __driver(ctx) as driver:
        records, _, _ = driver.execute_query("""
            MATCH (r:ETLRun:ETLTask) 
            WITH r, r.name AS name, r.uuid AS id, r.startTime AS startTime, r.endTime AS endTime
            CALL (r) {
              MATCH (r)-[:HAS_STATS]->(stats)
              WITH [k IN keys(stats) | stats[k] ] AS stats
              UNWIND stats AS stat
              RETURN sum(stat) AS changes
              }
            ORDER BY startTime DESC LIMIT $number_runs
            RETURN name, id, startTime, endTime, changes
        """, number_runs=number_runs, routing_=neo4j.RoutingControl.READ)
        data = [
            {
                "name": record["name"], "ID": record["id"],
                "startTime": __convert_date_time(record["startTime"]),
                "endTime": __convert_date_time(record["endTime"]),
                "changes": record["changes"]
            } for record in records]

        print(tabulate(data, headers='keys', tablefmt='psql'))


@cli.command()
@click.argument('run-id', required=True)
@click.option("--details", default=False, is_flag=True, help="Show stats for each task", type=bool)
@click.pass_context
def detail(ctx, run_id, details):
    """
    Show a breakdown of the task for the specified run, including statistics.
    """
    print(f"Showing details for run ID: {run_id}")
    with __driver(ctx) as driver:
        records, _, _ = driver.execute_query("""
        MATCH (r:ETLRun {uuid : $id})-[:HAS_SUB_TASK*]->(task) 
        WITH task ORDER BY task.order ASC
        CALL (task) {
          MATCH (task)-[:HAS_STATS]->(stats)
          WITH [k IN keys(stats) | stats[k] ] AS stats
          UNWIND stats AS stat
          RETURN sum(stat) AS changes
        }
        RETURN 
          task.task AS task, task.status AS status,
          task.batches + ' / ' + coalesce(task.expected_batches, '-') AS batches, 
          task.startTime AS startTime,  task.endTime AS endTime, changes
        """, id=run_id, routing_=neo4j.RoutingControl.READ)
        data = [
            {
                "task": record["task"],
                "status": record["status"],
                "batches": record["batches"],
                "duration": __duration_from_start_end(record["startTime"], record["endTime"]),
                "changes": record.get("changes", 0)
            }
            for record in records
        ]

        print(tabulate(data, headers='keys', tablefmt='psql'))
        if details:
            __print_details(driver, run_id)


# noinspection PyTypeChecker
@cli.command()
@click.option('--run-id', required=False, type=str, help='Run IDs to delete, works with comma separated list')
@click.option('--before', type=click.DateTime(formats=["%Y-%m-%d"]), help='Delete runs before a specific date in format YYYY-MM-DD')
@click.option('--older', help='Delete runs older than x days', type=int)
@click.pass_context
def delete(ctx, run_id, before, older):
    """
    Delete runs based on run ID, date, or age. One and only one of --run-id, --before, or --older must be provided.
    """
    # Ensure mutual exclusivity
    options = [run_id, before, older]
    if sum(bool(opt) for opt in options) != 1:
        print("You must specify exactly one of --run-id, --before, or --older.")
        return

    if run_id:
        ids = run_id.split(',')
        delete_runs(ctx, ids)
    elif before:
        print(f"Deleting runs before: {before}")
        with __driver(ctx) as driver:
            record= driver.execute_query(
                """MATCH (r:ETLRun) WHERE date(r.startTime) < date($before) 
                RETURN collect(r.uuid) AS ids
                """,
                result_transformer_=neo4j.Result.single,
                before=before)
            ids = record[0]
            delete_runs(ctx, ids)

    elif older:
        print(f"Deleting runs older than: {older}")
        with __driver(ctx) as driver:
            record = driver.execute_query(
                """MATCH (r:ETLRun) WHERE date(r.startTime) < (date() - duration({days: $days})) 
                RETURN collect(r.uuid) AS ids
                """,
                result_transformer_=neo4j.Result.single,
                days=older)
            ids = record[0]
            delete_runs(ctx, ids)


def delete_runs(ctx, ids):
    print(f"Deleting run IDs: {ids}")
    with __driver(ctx) as driver:
        records, _, _ = driver.execute_query(
            """
            MATCH (r:ETLRun)-[*]->(n) WHERE r.uuid IN $ids
            DETACH DELETE n
            DETACH DELETE r
            """, ids=ids, routing_=neo4j.RoutingControl.WRITE)
    print(f"Deleted run IDs: {ids}  successfully")
