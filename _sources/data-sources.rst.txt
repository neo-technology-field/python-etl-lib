Data Sources
============

Data sources are implementations of :class:`~etl_lib.core.BatchProcessor.BatchProcessor` and serve as the starting point in a chain of processors from source to sink. See :doc:`batching` for more details.

CSV
---

The :class:`~etl_lib.data_source.CSVBatchSource.CSVBatchSource` implementation enables reading CSV files in batches.

It utilizes Python's built-in `csv` module. The constructor forwards `kwargs` to `csv.DictReader`, allowing adaptation to different CSV formats.

Additionally, it detects compressed files and decompresses them on the fly.

Example usage:

.. code-block:: python

    from pathlib import Path

    csv_source = CSVBatchSource(Path("input.csv"), context, delimiter=';', quotechar='"')


The :class:`~etl_lib.task.data_loading.CSVLoad2Neo4jTask.CSVLoad2Neo4jTask` provides implementation that utilises the :class:`~etl_lib.data_source.CSVBatchSource.CSVBatchSource` together with Pydantic to stream data from CSV to Neo4j.

See the gtfs in examples for a demo.

Parquet
-------

The :class:`~etl_lib.data_source.ParquetBatchSource.ParquetBatchSource` implementation enables reading Parquet files in batches.

It utilizes the `pyarrow <https://arrow.apache.org/docs/python/>`_ library. The constructor forwards `kwargs` to `pyarrow.parquet.ParquetFile.iter_batches`, allowing customization of the reading process.

This data source requires the optional `parquet` dependency:

.. code-block:: console

    pip install "neo4j-etl-lib[parquet]"

Example usage:

.. code-block:: python

    from pathlib import Path
    from etl_lib.data_source.ParquetBatchSource import ParquetBatchSource

    parquet_source = ParquetBatchSource(Path("input.parquet"), context)

The library provides two task implementations for loading Parquet data:

* :class:`~etl_lib.task.data_loading.ParquetLoad2Neo4jTask.ParquetLoad2Neo4jTask`: For sequential loading.
* :class:`~etl_lib.task.data_loading.ParallelParquetLoad2Neo4jTask.ParallelParquetLoad2Neo4jTask`: For parallel loading using the mix-and-batch strategy.


Neo4j / Cypher
--------------

The :class:`~etl_lib.data_source.CypherBatchSource.CypherBatchSource` implementation streams query results in batches while keeping the transaction open until all data has been returned.

Each row in the returned batch is a dictionary, as provided by the `Neo4j Python Driver <https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Record.data>`_.

If an optional `record_transformer` is supplied in the constructor, this transformer is used to convert the `Record` into a dictionary, providing greater flexibility in handling different data types.

Example usage:

.. code-block:: python

    query = "MATCH (n) RETURN n.id AS id, n.name AS name, n.type AS type"
    cypher_source = CypherBatchSource(context, task, query)


Relational Databases / SQL
--------------------------

The :class:`~etl_lib.data_source.SQLBatchSource.SQLBatchSource` implementation streams query results in batches while keeping the transaction open until all data has been returned.

Each row in the returned batch is a dictionary.

This datasource is only enabled if the module ``sqlalchemy`` is installed.
The connection url is expected in the environment variable ``SQLALCHEMY_URI``.

Using the below classes, the implementation of 2 functions is needed: one return the SQL query to retrieve the data and another to return the Cypher query to write the data into Neo4j.

The library provides two task implementations for loading from SQL databases:

* :class:`~etl_lib.task.data_loading.SQLLoad2Neo4jTask.SQLLoad2Neo4jTask` : For sequential loading.
* :class:`~etl_lib.task.data_loading.SQLLoad2Neo4jTask.ParallelSQLLoad2Neo4jTask` :For parallel loading using the mix-and-batch strategy.

See the Musikbrainz demo in examples the examples folder.
