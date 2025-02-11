Configuration
=============

All parts of an ETL pipeline have access to the :class:`~etl_lib.core.ETLContext.ETLContext` class and can retrieve configuration parameters via :func:`~etl_lib.core.ETLContext.ETLContext.env`.

This configuration is backed by a dictionary passed to the context's constructor. The following code demonstrates how to use environment variables to populate this dictionary:

.. code-block:: python

    context = ETLContext(env_vars=dict(os.environ))

Using environment variables makes it easy to configure the ETL pipeline externally.

The following parameters are currently recognized:

.. list-table:: Configuration Parameters
    :header-rows: 1

    * - Name
      - Domain
      - Description
    * - ``NEO4J_URI``
      - Neo4j Connection
      - Connection URL, such as ``neo4j://localhost:7687``
    * - ``NEO4J_USERNAME``
      - Neo4j Connection
      - Database user that the ETL pipeline will use
    * - ``NEO4J_PASSWORD``
      - Neo4j Connection
      - Password for the specified database user
    * - ``NEO4J_DATABASE``
      - Neo4j Connection
      - Name of the database to use during the ETL pipeline
    * - ``REPORTER_DATABASE``
      - Reporting
      - | Name of the database to store ETL metadata.
        | See :ref:`neo4j_reporter` for more details. If not provided,
        | reporting will be done only to the console or a log file.
