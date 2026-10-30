Building pipelines
==================

This page explains how the parts of the library are meant to be combined. The reference pages describe each class on its own. This page describes the whole.

What a pipeline is made of
--------------------------

There are two axes of composition. One for the pipeline as a whole and one for each source to sink chain.

A :py:class:`~etl_lib.core.Task.TaskGroup` sequences work. Its tasks run in the order given, and a group can contain other groups.
A pipeline is a tree of tasks with a group at the root, started by calling :py:func:`~etl_lib.core.Task.Task.execute` on the root.
Statistics from each :py:class:`~etl_lib.core.Task.TaskReturn` are merged upwards, so grouping also decides how the reporting reads.

A chain of :py:class:`~etl_lib.core.BatchProcessor.BatchProcessor` instances moves data within one task.
Each processor pulls batches from its predecessor: a source at the start, optional stages such as validation in the middle, a sink at the end.
The chain is how data flows in batches from the source to the sink. A chain is implemented as a `Task`.

Use a group to order tasks. Use a processor chain to stream one load. The load tasks described below already assemble the chain for you.

How a load task works
---------------------

For each source format there is a ready made task, for example :py:class:`~etl_lib.task.data_loading.CSVLoad2Neo4jTask.CSVLoad2Neo4jTask`. Building a load means subclassing it and filling in two things: the Cypher that writes a batch, and optionally a Pydantic model that validates each row. The SQL task is the exception: it implements ``_sql_query()`` and ``_cypher_query()`` instead of ``_query()``, and takes no validation model.

.. code-block:: python

    class LoadStopsTask(CSVLoad2Neo4jTask):  # (1)

        class Stop(BaseModel):  # (2)
            id: str = Field(alias="stop_id")
            name: str = Field(alias="stop_name")

        def __init__(self, context: ETLContext, file: Path):
            super().__init__(context, file, model=LoadStopsTask.Stop, batch_size=1000)  # (3)

        def _query(self):  # (4)
            return """
            UNWIND $batch AS row
            MERGE (s:Stop {id: row.id})
            SET s.name = row.name
            """

1. Subclass the task matching the source format.
2. The Pydantic model is optional. Rows that fail validation are written to the error file and do not reach the database. See :doc:`validation`.
3. File, Pydantic model and ``batch_size``. The error file location comes from the ``ETL_ERROR_PATH`` context setting or the input file name, see :doc:`validation`.
4. The Cypher query is the mapping. It is written as a literal next to the task, and it operates on ``$batch``, the list of rows for the current batch.

Where does the logic live
-------------------------

When a row is not in the shape the graph needs, there are three places to fix it. In order of preference:

1. In the Cypher query. Deduplication, lookups, defaults that depend on graph state.
2. In the Pydantic model. Row shape, types, aliases, defaults.
3. In a custom :py:class:`~etl_lib.core.BatchProcessor.BatchProcessor` stage. Only when neither of the above can express it.

Loading in the right order
--------------------------

A relationship task that ``MATCH``es nodes created by other tasks must run after them.
Otherwise the ``MATCH`` finds nothing and the row writes nothing, without an error.
A task that creates or merges the nodes in the same statement has no such constraint and can write nodes and relationships in one go.

The gtfs example uses separate groups: one for the node files, one for the relationship files:

.. code-block:: python

    context = ETLContext(env_vars=dict(os.environ))
    nodes = TaskGroup(context=context, name="nodes",
                      tasks=[LoadStopsTask(context, stops_file),
                             LoadRoutesTask(context, routes_file)])
    edges = TaskGroup(context=context, name="relationships",
                      tasks=[LoadStopTimesTask(context, stop_times_file)])
    pipeline = TaskGroup(context=context, name="import", tasks=[nodes, edges])
    result = pipeline.execute()

A complete example of this shape, including the click based CLI around it, is the `gtfs example <https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/gtfs>`__.

Loading relationships
---------------------

Relationships that share nodes can not be written in parallel. Concurrent transactions lock the same nodes and deadlock. :doc:`parallel` describes the mix and batch strategy that works around this.

Testing a pipeline
------------------

The library ships its own test support. :mod:`etl_lib.test_utils` provides the fixtures and mocks to stand up an :py:class:`~etl_lib.core.ETLContext.ETLContext` against a test database, including a throwaway database per run via ``NEO4J_TEST_DATABASE``. See :doc:`testing`.
