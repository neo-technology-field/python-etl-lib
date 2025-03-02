# Example pipeline to load a GTFS feed

This directory contains an example pipeline to load https://gtfs.org/documentation/schedule/reference/ [GTFS]feed data into Neo4j. The data model is re-used from the https://faboo.org/2021/01/loading-uk-gtfs/[Loading the UK GTFS data feed]
blog post, rewriting the `LOAD CSV` used there to build a pipeline as a showcase.

To run it, a GTFS feed needs to be placed into a directory. See https://gtfs.de/en/feeds/ for downloads.

The `gtfs.py` is mostly about setting up a cli and loading/reading arguments and env variables.

The actual pipeline is constructed via:


```python
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

context.reporter.register_tasks(all_group, source=input_directory.name)

all_group.execute()
```

An `all_group` is constructed from the `TaskGroup` `init_group, csv_group, post_group]` which again consists of other Task.

The `all_group.execute()` triggers the pipeline. The Tasks execution follows the order of the task in the arrays.

Validating the input data is done via Pydantic. See `Load*Task.py` for examples. Lines not matching the Pydantic rules are written as `*.error.json` per input file and can be examined after execution.

Tasks have the ability to abort the entire pipeline by implementing the `abort_on_fail()` function and return `True`.

Example output from the GTFS example is shown below:

[source,python,options="nowrap"]
```
025-01-18 18:44:45,214 - INFO - Processing directory: /Users/bert/Downloads/mdb-2333-202412230030
2025-01-18 18:44:45,214 - INFO - Neo4j URL: neo4j://localhost:7687
2025-01-18 18:44:45,214 - INFO - Neo4j User: neo4j
2025-01-18 18:44:45,214 - INFO - Neo4j Database Name: neo4j
2025-01-18 18:44:45,214 - INFO - Connecting to Neo4j at neo4j://localhost:7687 with user neo4j to access database neo4j...
2025-01-18 18:44:45,239 - INFO - driver connected to instance at neo4j://localhost:7687 with username neo4j and database neo4j
2025-01-18 18:44:45,239 - INFO -
└──main
   ├──schema-init
   │  └──SchemaTask
   ├──csv-loading
   │  ├──LoadAgenciesTask('/Users/bert/Downloads/mdb-2333-202412230030/agency.txt')
   │  ├──LoadRoutesTask('/Users/bert/Downloads/mdb-2333-202412230030/routes.txt')
   │  ├──LoadStopsTask('/Users/bert/Downloads/mdb-2333-202412230030/stops.txt')
   │  ├──LoadTripsTask('/Users/bert/Downloads/mdb-2333-202412230030/trips.txt')
   │  ├──LoadCalendarTask('/Users/bert/Downloads/mdb-2333-202412230030/calendar.txt')
   │  └──LoadStopTimesTask('/Users/bert/Downloads/mdb-2333-202412230030/stop_times.txt')
   └──post-processing
      └──CreateSequenceTask

2025-01-18 18:44:45,715 - INFO - starting main
2025-01-18 18:44:45,752 - INFO - starting schema-init
2025-01-18 18:44:45,757 - INFO - starting SchemaTask
2025-01-18 18:44:45,871 - INFO - finished SchemaTask with success: True
+-----------------+---------------------+
|   indexes_added |   constraints_added |
|-----------------+---------------------|
|               3 |                   4 |
+-----------------+---------------------+
2025-01-18 18:44:46,017 - INFO - finished schema-init with success: True
+---------------------+-----------------+
|   constraints_added |   indexes_added |
|---------------------+-----------------|
|                   4 |               3 |
+---------------------+-----------------+
2025-01-18 18:44:46,024 - INFO - starting csv-loading
2025-01-18 18:44:46,048 - INFO - starting LoadAgenciesTask('/Users/bert/Downloads/mdb-2333-202412230030/agency.txt')
2025-01-18 18:44:46,178 - INFO - finished LoadAgenciesTask('/Users/bert/Downloads/mdb-2333-202412230030/agency.txt') with success: True
+----------------+-----------------+------------------+--------------+------------------+
|   labels_added |   nodes_created |   properties_set |   valid_rows |   csv_lines_read |
|----------------+-----------------+------------------+--------------+------------------|
|              1 |               1 |                5 |            1 |                1 |
+----------------+-----------------+------------------+--------------+------------------+
2025-01-18 18:44:46,184 - INFO - starting LoadRoutesTask('/Users/bert/Downloads/mdb-2333-202412230030/routes.txt')
2025-01-18 18:44:46,417 - INFO - finished LoadRoutesTask('/Users/bert/Downloads/mdb-2333-202412230030/routes.txt') with success: True
+----------------+-----------------+------------------+--------------+------------------+-------------------------+
|   labels_added |   nodes_created |   properties_set |   valid_rows |   csv_lines_read |   relationships_created |
|----------------+-----------------+------------------+--------------+------------------+-------------------------|
|            299 |             299 |             1196 |          299 |              299 |                     299 |
+----------------+-----------------+------------------+--------------+------------------+-------------------------+
2025-01-18 18:44:46,423 - INFO - starting LoadStopsTask('/Users/bert/Downloads/mdb-2333-202412230030/stops.txt')
2025-01-18 18:44:46,870 - INFO - finished LoadStopsTask('/Users/bert/Downloads/mdb-2333-202412230030/stops.txt') with success: True
+----------------+-----------------+------------------+--------------+------------------+
|   labels_added |   nodes_created |   properties_set |   valid_rows |   csv_lines_read |
|----------------+-----------------+------------------+--------------+------------------|
|           4170 |            4170 |            25019 |         4170 |             4170 |
+----------------+-----------------+------------------+--------------+------------------+
2025-01-18 18:44:46,875 - INFO - starting LoadTripsTask('/Users/bert/Downloads/mdb-2333-202412230030/trips.txt')
2025-01-18 18:44:51,782 - INFO - finished LoadTripsTask('/Users/bert/Downloads/mdb-2333-202412230030/trips.txt') with success: True
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
|   properties_set |   relationships_created |   labels_added |   nodes_created |   valid_rows |   csv_lines_read |
|------------------+-------------------------+----------------+-----------------+--------------+------------------|
|           614069 |                   91694 |          91694 |           91694 |        91694 |            91694 |
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
2025-01-18 18:44:51,786 - INFO - starting LoadCalendarTask('/Users/bert/Downloads/mdb-2333-202412230030/calendar.txt')
2025-01-18 18:44:52,262 - INFO - finished LoadCalendarTask('/Users/bert/Downloads/mdb-2333-202412230030/calendar.txt') with success: True
+----------------+--------------+------------------+
|   labels_added |   valid_rows |   csv_lines_read |
|----------------+--------------+------------------|
|         198095 |          212 |              212 |
+----------------+--------------+------------------+
2025-01-18 18:44:52,266 - INFO - starting LoadStopTimesTask('/Users/bert/Downloads/mdb-2333-202412230030/stop_times.txt')
2025-01-18 18:46:22,633 - INFO - finished LoadStopTimesTask('/Users/bert/Downloads/mdb-2333-202412230030/stop_times.txt') with success: True
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
|   properties_set |   relationships_created |   labels_added |   nodes_created |   valid_rows |   csv_lines_read |
|------------------+-------------------------+----------------+-----------------+--------------+------------------|
|          9494080 |                 3797632 |        1898816 |         1898816 |      1898816 |          1898816 |
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
2025-01-18 18:46:22,653 - INFO - finished csv-loading with success: True
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
|   properties_set |   relationships_created |   labels_added |   nodes_created |   valid_rows |   csv_lines_read |
|------------------+-------------------------+----------------+-----------------+--------------+------------------|
|         10134369 |                 3889625 |        2193075 |         1994980 |      1995192 |          1995192 |
+------------------+-------------------------+----------------+-----------------+--------------+------------------+
2025-01-18 18:46:22,655 - INFO - starting post-processing
2025-01-18 18:46:22,668 - INFO - starting CreateSequenceTask
2025-01-18 18:46:32,888 - INFO - finished CreateSequenceTask with success: True
2025-01-18 18:46:32,892 - INFO - finished post-processing with success: True
2025-01-18 18:46:32,894 - INFO - finished main with success: True
+------------------+-----------------+-------------------------+----------------+-----------------+---------------------+--------------+------------------+
|   properties_set |   indexes_added |   relationships_created |   labels_added |   nodes_created |   constraints_added |   valid_rows |   csv_lines_read |
|------------------+-----------------+-------------------------+----------------+-----------------+---------------------+--------------+------------------|
|         10134369 |               3 |                 3889625 |        2193075 |         1994980 |                   4 |      1995192 |          1995192 |
+------------------+-----------------+-------------------------+----------------+-----------------+---------------------+--------------+------------------+
2025-01-18 18:46:32,897 - INFO - Processing complete.
```

When reporting to a Neo4j database, each ETL run results in a tree structure like the one shown below (example from the GTFS example):

![Schema](../../docs/_static/images/schema.png)

Each ETLTask node, once the associated task has been completed, will have an attached `ETLStats` node with properties such as below. The stats reported here depend on the task(s) involved.


```
csv_lines_read:4170,
labels_removed:0,
indexes_removed:0,
constraints_added:0,
relationships_created:0,
nodes_deleted:0,
indexes_added:0,
relationships_deleted:0,
properties_set:20850,
invalid_rows:0,
constraints_removed:0,
labels_added:0,
nodes_created:0,
valid_rows:4170
```

Tasks with SubTasks (defined as `TaskGroup`) aggregate statistics from all their child Tasks. Therefore, viewing the top-level `ETLRun` node provides a summary of the entire pipeline.
