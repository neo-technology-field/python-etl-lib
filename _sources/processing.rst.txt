Data Processing
===============

Processing of data is supported through the following tasks:

Cypher Execution
----------------

The :class:`~etl_lib.task.ExecuteCypherTask.ExecuteCypherTask` supports in running a given Cypher statement while capturing the usual statistics like nodes created, deleted, properties set and so and sending the statistics to the :class:`~etl_lib.core.ProgressReporter.ProgressReporter`. This can be used for data aggregations, clean ups and so on.

The task calls ``_query()`` once per execution. Returning a list of statements runs them in order through a single session and sums the statistics. Each statement gets its own managed transaction, so the list is not atomic. A statement that fails leaves the statements before it applied. Return a single statement when the work has to be all or nothing.

Cypher parameters are passed to ``execute()`` or ``run_internal()`` as keyword arguments and are bound as ``$`` parameters. They are not constructor arguments.

Schema Setup
------------

Schema changes for :class:`~etl_lib.task.SchemaTask.SchemaTask` run when the task is executed. Building the context or starting the application applies none of the statements the task supplies.

That is not a statement about the whole library. When ``REPORTER_DATABASE`` selects the Neo4j reporter, constructing the context also creates that reporter's own uniqueness constraint.

Subclass :class:`~etl_lib.task.SchemaTask.SchemaTask` and return the statements from ``_schema_statements()``:

.. code-block:: python

    from etl_lib.task.SchemaTask import SchemaTask

    class ExampleSchema(SchemaTask):
        def _schema_statements(self) -> list[str]:
            return [
                """CREATE CONSTRAINT example_id IF NOT EXISTS
                FOR (n:Example) REQUIRE n.id IS UNIQUE""",
            ]

    ExampleSchema(context).execute()

:py:meth:`~etl_lib.task.SchemaTask.SchemaTask.from_file` keeps the statements in a file and reads it when the task runs. The public constructor still only takes the context, the path is wiring rather than part of the task state:

.. code-block:: python

    from pathlib import Path

    schema = SchemaTask.from_file(context=context, path=Path("schema.cypher"))

Files are UTF-8 and hold Cypher statements separated by semicolons. The final statement can omit its terminator. Line comments (``//``) and block comments (``/* */``) are removed, and a semicolon inside a quoted string, a quoted identifier or a comment does not end a statement.

An empty file, or one holding only comments, is a successful no-op with an empty summary.

The splitter is lexical, not a Cypher parser. It does not validate statement syntax, does not understand shell commands such as ``:use``, and does not support include directives or migration directives.

Schema statements are not made idempotent for the caller. Supply ``IF NOT EXISTS`` yourself, otherwise a second run fails on the constraint or index that already exists.

A missing file raises ``FileNotFoundError``. An unterminated quote or block comment raises ``ValueError``. Both are raised from ``run_internal()`` and handled by the usual :py:meth:`~etl_lib.core.Task.Task.execute` failure lifecycle, so the task returns an unsuccessful result with the error attached instead of aborting the process.

GDS Task
--------

The :class:`~etl_lib.task.GDSTask.GDSTask` uses the `GDS client <https://neo4j.com/docs/graph-data-science-client/current/>`_ to run Graph Data Science jobs. To use it, you need to provide a function to the constructor that implements the logic. This function gets the :class:`~etl_lib.core.ETLContext.ETLContext` passed in. From the context, an instance of the gds client can be obtained::


        def gds_fun(etl_context):
            gds =  etl_context.neo4j.gds
            gds.graph.drop("neo4j-offices", failIfMissing=False)
            g_office, project_result = gds.graph.project("neo4j-offices", "City", "FLY_TO")
            mutate_result = gds.pageRank.write(g_office, tolerance=0.5, writeProperty="rank")
            return TaskReturn(success=True, summary=transform_dict(mutate_result.to_dict()))


.. DANGER::
    Do *not* use the obtained gds context with a context manager, as this will close the gds client and the contained Neo4j connection on leaving the context.



