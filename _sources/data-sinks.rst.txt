Data Sinks
==========

Data sinks are implementations of :class:`~etl_lib.core.BatchProcessor.BatchProcessor` and serve as the endpoint in a chain of processors, writing data to a final destination. See :doc:`batching` for more details.

Data to write is acquired from the provided predecessor in batches.

CSV
---

The :class:`~etl_lib.data_sink.CSVBatchSink.CSVBatchSink` implementation enables writing batches of data to a CSV file.

It uses Python's built-in `csv` module and allows customization via additional arguments passed to `csv.DictWriter`.

**Behavior:**
- If the specified CSV file exists, data will be appended.
- It automatically detects and writes headers if the file is new.

Example usage:

.. code-block:: python

    csv_sink = CSVBatchSink(context, task, predecessor, Path("output.csv"))

Neo4j / Cypher
--------------

The :class:`~etl_lib.data_sink.CypherBatchSink.CypherBatchSink` implementation writes batch data to a Neo4j database using Cypher queries.

**Behavior:**
- Each batch is written in its own transaction.
- Data is passed using the `batch` parameter, requiring Cypher queries to begin with `UNWIND $batch AS row`.

Example usage:

.. code-block:: python

    cypher_sink = CypherBatchSink(context, task, predecessor, """
        UNWIND $batch AS row
        MERGE (n:Entity {id: row.id})
        SET n += row
    """)


SQL
---

The :class:`~etl_lib.data_sink.CypherBatchSink.SQLBatchSink` writes batches of data to a SQL database using the provided SQL query.
