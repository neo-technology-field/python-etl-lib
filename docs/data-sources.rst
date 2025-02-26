Data Sources
============

Data sources are implementations of :class:`~etl_lib.core.BatchProcessor.BatchProcessor` and serve as the starting point in a chain of processors from source to sink. See :doc:`batching` for more details.

CSV
---

The :class:`~etl_lib.data_source.CSVBatchSource.CSVBatchSource` implementation enables reading CSV files in batches.

It utilizes Python's built-in `csv` module. The constructor forwards `kwargs` to `csv.DictReader`, allowing adaptation to different CSV formats.

Additionally, it detects compressed files and decompresses them on the fly.

Neo4j / Cypher
--------------

The :class:`~etl_lib.data_source.CypherBatchSource.CypherBatchSource` implementation streams query results in batches while keeping the transaction open until all data has been returned.

Each row in the returned batch is a dictionary, as provided by the `Neo4j Python Driver <https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Record.data>`_.

If an optional `record_transformer` is supplied in the constructor, this transformer is used to convert the `Record` into a dictionary, providing greater flexibility in handling different data types.
