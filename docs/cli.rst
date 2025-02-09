CLI
===

The module :mod:`etl_lib.cli.run_tools` defines a few functions to to query details of past etl runs. They make use of the ``click`` package to define a click ``group()`` with the name ``cli`` that can be easily added to a command line utility.

See the `GTFS example <https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/gtfs>`_ on how to build a command line tool.

.. code-block:: python

    from etl_lib.cli.run_tools import cli
    @cli.command("<your own command>")
    @click.argument(<cour own arguments>)
    @click.pass_context
    def main(ctx, input_directory):
        pass

    if __name__ == '__main__':
        cli()

Commands
--------

The above example integrates the ``cli`` group. The help output of the added commands is as follows:

.. code-block:: console

    $bash: python <your-cli>.py --help
    Usage: <your-cli>.py [OPTIONS] COMMAND [ARGS]...

      Environment variables can be configured via a .env file or overridden via
      CLI options:

      - NEO4J_URI: Neo4j database URI
      - NEO4J_USERNAME: Neo4j username
      - NEO4J_PASSWORD: Neo4j password
      - LOG_FILE: Path to the log file
      - DATABASE_NAME: Neo4j database name (default: neo4j)

    Options:
      --neo4j-uri TEXT       Neo4j database URI
      --neo4j-user TEXT      Neo4j username
      --neo4j-password TEXT  Neo4j password
      --log-file TEXT        Path to the log file
      --database-name TEXT   Neo4j database name (default: neo4j)
      --help                 Show this message and exit.

    Commands:
      delete  Delete runs based on run ID, date, or age.
      detail  Show a breakdown of the task for the specified run, including...
      query   Retrieve the list of the last x etl runs from the database and...

Query
+++++

.. code-block:: console

    $bash: python <your-cli>.py query --help
    Usage: <your-cli>.py query [OPTIONS]

    Retrieve the list of the last x etl runs from the database and display them.

    Options:
      --number-runs INTEGER  Number of rows to process, defaults to 10
      --help                 Show this message and exit.

.. code-block:: console

    <your-cli>.py query
    Listing runs in database 'neo4j'
    +--------+--------------------------------------+------------------+------------------+-----------+
    | name   | ID                                   | startTime        | endTime          |   changes |
    |--------+--------------------------------------+------------------+------------------+-----------|
    | main   | 69260954-0b94-4043-be1b-f99ce5a64d3a | 2025-02-09 17:19 | 2025-02-09 17:20 |   4566469 |
    +--------+--------------------------------------+------------------+------------------+-----------+

The ``changes`` column is the sum of all changes in that run, be it csv rows read, constraints added, property set, ..

Detail
++++++

.. code-block:: console

    $bash: python <your-cli>.py detail --help
    Usage: <your-cli>.py detail [OPTIONS] RUN_ID

      Show a breakdown of the task for the specified run, including statistics.

    Options:
      --details  Show stats for each task
      --help     Show this message and exit.

.. code-block:: console

    $bash: python <your-cli>.py detail 69260954-0b94-4043-be1b-f99ce5a64d3a
    Showing details for run ID: 69260954-0b94-4043-be1b-f99ce5a64d3a
    +-------------------------------------------------------------------------------+----------+-----------+------------+-----------+
    | task                                                                          | status   | batches   | duration   |   changes |
    |-------------------------------------------------------------------------------+----------+-----------+------------+-----------|
    | TaskGroup(schema-init)                                                        | success  |           | 0:00:00    |         0 |
    | Task(SchemaTask)                                                              | success  |           | 0:00:00    |         0 |
    | TaskGroup(csv-loading)                                                        | success  |           | 0:00:57    |   4566469 |
    | LoadAgenciesTask(/Users/bert/Downloads/mdb-2333-202412230030/agency.txt)      | success  | 1 / -     | 0:00:00    |         6 |
    | LoadRoutesTask(/Users/bert/Downloads/mdb-2333-202412230030/routes.txt)        | success  | 1 / -     | 0:00:00    |      1495 |
    | LoadStopsTask(/Users/bert/Downloads/mdb-2333-202412230030/stops.txt)          | success  | 1 / -     | 0:00:00    |     33360 |
    | LoadTripsTask(/Users/bert/Downloads/mdb-2333-202412230030/trips.txt)          | success  | 19 / -    | 0:00:03    |    733552 |
    | LoadCalendarTask(/Users/bert/Downloads/mdb-2333-202412230030/calendar.txt)    | success  | 1 / -     | 0:00:00    |       424 |
    | LoadStopTimesTask(/Users/bert/Downloads/mdb-2333-202412230030/stop_times.txt) | success  | 380 / -   | 0:00:54    |   3797632 |
    | TaskGroup(post-processing)                                                    | success  |           | 0:00:07    |         0 |
    | Task(CreateSequenceTask)                                                      | success  |           | 0:00:07    |         0 |
    +-------------------------------------------------------------------------------+----------+-----------+------------+-----------+

In the above example, the expected total of batches was not know in advance, hence the ``380 / -`` display. Read tis as `380 batches of unknown`.

With an additional ``--details`` flag, for each task in the table above detailed information will be displayed (only showing 1 row):

.. code-block:: console

    $bash: python <your-cli>.py detail 69260954-0b94-4043-be1b-f99ce5a64d3a --details
    Showing statistics for Task 'TaskGroup(csv-loading)' with status 'success'
    +----------------+---------+
    | Name           |   Value |
    |----------------+---------|
    | csv_lines_read | 1995192 |
    | properties_set |  576085 |
    | valid_rows     | 1995192 |
    +----------------+---------+


Delete
++++++

.. code-block:: console

    $bash: python <your-cli>.py delete --help
    Usage: <your-cli>.py delete [OPTIONS]

      Delete runs based on run ID, date, or age. One and only one of --run-id,
      --since, or --older must be provided.

    Options:
      --run-id TEXT  Run ID to delete
      --since TEXT   Delete runs since a specific date
      --older TEXT   Delete runs older than a specific date
      --help         Show this message and exit.
