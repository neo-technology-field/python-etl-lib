Data Processing
===============

Processing of data can is support through the following two tasks:

Cypher Execution
----------------

The :class:`~etl_lib.task.ExecuteCypherTask.ExecuteCypherTask` supports in running a given Cypher statement while capturing the usual statistics like nodes created, deleted, properties set and so and sending the statistics to the :class:`~etl_lib.core.ProgressReporter.ProgressReporter`. This can be used for data aggregations, clean ups and so on.

GDS Task
--------

The :class:`~etl_lib.task.GDSTask.GDSTask` uses the `GDS client <https://neo4j.com/docs/graph-data-science-client/current/>`_ to run Graph Data Science jobs. To use it, you need to provide a function to the constructor that implements the logic. This function gets the :class:`~etl_lib.core.ETLContext.ETLContext` passed in. From the context, an instance of the gds client can be obtained::


        def gds_fun(etl_context):
            gds =  etl_context.neo4j.gds
            gds.graph.drop("neo4j-offices", failIfMissing=False)
            g_office, project_result = gds.graph.project("neo4j-offices", "City", "FLY_TO")
            mutate_result = gds.pageRank.write(g_office, tolerance=0.5, writeProperty="rank")
            return TaskReturn(success=True, summery=transform_dict(mutate_result.to_dict()))


.. DANGER::
    Do *not* use the obtained gds context with a context manager, as this will close the gds client and the contained Neo4j connection on leaving the context.



