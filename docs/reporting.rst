Reporting
=========

The library comes with 2 implementations for reporting.


..  _basic_reporter:

Basic Reporter
--------------

The :class:`~etl_lib.core.ProgressReporter.ProgressReporter` implements basic reporting using the Python logging package. Logging is done on ``INFO`` level.

The :func:`~etl_lib.core.utils.setup_logging` function is provided and sets up the logging to console and optional file.

The reporter is created and configured inside the :class:`~etl_lib.core.ETLContext.ETLContext` constructor and can be retrieved from there.

Part of the etl pipeline setup must be a call to the :func:`~etl_lib.core.ProgressReporter.ProgressReporter.register_tasks()` passing in the root of the task tree:

.. code-block:: python

    context.reporter.register_tasks(all_group)

This will log a tree representation of the tasks:

.. code-block::

    2025-02-09 13:33:42,696 - INFO -
    └──main
       ├──schema-init
       │  └──SchemaTask
       ├──csv-loading
       │  ├──LoadAgenciesTask('mdb-2333-202412230030/agency.txt')
       │  ├──LoadRoutesTask('mdb-2333-202412230030/routes.txt')
       │  ├──LoadStopsTask('mdb-2333-202412230030/stops.txt')
       │  ├──LoadTripsTask('mdb-2333-202412230030/trips.txt')
       │  ├──LoadCalendarTask('mdb-2333-202412230030/calendar.txt')
       │  └──LoadStopTimesTask('mdb-2333-202412230030/stop_times.txt')
       └──post-processing
          └──CreateSequenceTask

The :class:`~etl_lib.core.Task.Task` is responsible for informing the reporter (from the context) about status changes, such as starting and ending of tasks. This is done before and after calls to :func:`~etl_lib.core.Task.Task.run_internal()` so that implementations of :class:`~etl_lib.core.Task.Task` do not need to worry about this.

At the end of each :class:`~etl_lib.core.Task.Task`, the reporter will be called again leading to log messages such as the following:

.. code-block::

    2025-02-09 13:43:41,535 - INFO -  finished LoadStopTimesTask('mdb-2333-202412230030/stop_times.txt') in 0:01:03.138719 with success: True
    +--------------+------------------+------------------+----------------+-------------------------+-----------------+
    |   valid_rows |   csv_lines_read |   properties_set |   labels_added |   relationships_created |   nodes_created |
    |--------------+------------------+------------------+----------------+-------------------------+-----------------|
    |      1898816 |          1898816 |          3394080 |         678816 |                 1357632 |          678816 |
    +--------------+------------------+------------------+----------------+-------------------------+-----------------+

To keep the reporting readable, only summery information with non-zero values are shown in here.

Neo4j Reporter
--------------

The :class:`~etl_lib.core.ProgressReporter.Neo4jProgressReporter` extends the basic:class:`~etl_lib.core.ProgressReporter.ProgressReporter` so that each run of the of the pipeline is persisted into a Neo4j database.

Which implementation is used is decided by existence of the key ``REPORTER_DATABASE`` in ``context.env``. This ``env`` dictionary should be build from the process environment.

If the key ``REPORTER_DATABASE`` exists, its value will be used to write etl status information into the specified database, thus allowing to separate the application data from etl status data.

Each ETL run create a sub graph that is only connected to itself. The following image shows the structure of such a sub grap for the GTFS example:

.. image:: _static/images/schema.png

The green colors denote :class:`~etl_lib.core.Task.Task` and :class:`~etl_lib.core.Task.TaskGroups` are shown in blue.
The ``ETLStats`` nodes are created once a task is finished and hold the summery information for the attached task. For tasks that have children, this ``ETLStats`` data is the aggregation of of all children. Therefore, to see the summery for the pipeline run, the ``ETLStats`` attached to the ``ETLRun`` node is sufficient.

Here, all summery information is stored, meaning even entries with a ``0`` as value will be found in the ``ETLStats`` nodes (as opposed to the :ref:`basic_reporter` which only reports non-``0`` values).

The root of the project contains ``dashboard.json`` for a `Neodash Dashboard Builder <https://github.com/neo4j-labs/neodash>`_.

.. ATTENTION::
    The Neo4j reporter does not automatically create constraints for the meta data information. Such a constraint can be created manually via:
        .. code-block:: cypher

            CREATE CONSTRAINT IF NOT EXISTS FOR (n:ETLTask)
                REQUIRE n.uuid IS UNIQUE

    Alternatively, the :class:`~etl_lib.task.CreateReportingConstraintsTask.CreateReportingConstraintsTask` can be added to the beginning of a pipeline to create the constraint it does not exist.

In addition, :doc:`cli` explains how to use a command line interface to query and manage etl run history.

