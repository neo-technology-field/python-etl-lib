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
    * - ``ETL_ERROR_PATH``
      - Validation
      - | Directory where error files should be created.
        | See :doc:`validation` for more details. If not provided, error files will be placed into the same directory as the input files.
    * - ``NEO4J_TEST_CONTAINER``
      - Testing
      - | Docker image name to use for testing, esp.: ``neo4j:5.26.1-enterprise``.
        | See :doc:`testing` for more details. If provided, `TestContainers <https://testcontainers-python.readthedocs.io/en/latest/>`_
        | will be used with the image name provided.
    * - ``NEO4J_TEST_DATABASE``
      - Testing
      - | Name of the Neo4j database to use during integration testing.
        | Only considered if ``NEO4J_TEST_CONTAINER`` is not given.
        | Allows to run integration tests against an external Neo4j installation
        | without impacting other DBs.

