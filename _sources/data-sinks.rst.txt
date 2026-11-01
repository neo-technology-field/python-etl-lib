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

JSON
----

The :class:`~etl_lib.data_sink.JSONBatchSink.JSONBatchSink` implementation writes batches of data to a JSON file.

The ``format`` argument selects the output layout.

* ``jsonl``, the default, writes one object per line and appends to an existing file, so repeated runs add to the same file.
* ``array`` writes a single JSON array and always starts a new file, because an array cannot be extended record by record while staying valid JSON. The closing bracket is written when the batch stream ends or is closed, so an interrupted run still leaves a parsable file.

Remaining arguments are passed to `json.dumps`, so ``indent`` and ``ensure_ascii=False`` behave as usual.
``indent`` produces multi-line records and therefore no longer valid JSON Lines.

Example usage:

.. code-block:: python

    json_sink = JSONBatchSink(context, task, predecessor, Path("output.jsonl"))

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
