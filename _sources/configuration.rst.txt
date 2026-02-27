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


Neo4j Driver Configuration
--------------------------

You can pass configuration options directly to the Neo4j Python Driver by using environment variables prefixed with ``NEO4J_DRIVER_``.
The prefix is stripped, the name is lowercased, and the value is parsed to the appropriate type before being passed to the driver constructor.

See the `Neo4j Python Driver API documentation <https://neo4j.com/docs/api/python-driver/current/api.html#graphdatabase>`_ for a complete list of valid options and their meanings.

.. list-table:: Driver Configuration Options
    :header-rows: 1
    :widths: 40 10 50

    * - Name
      - Type
      - Description
    * - ``NEO4J_DRIVER_MAX_CONNECTION_POOL_SIZE``
      - int
      - The maximum total number of connections allowed, per host, to be managed by the connection pool.
    * - ``NEO4J_DRIVER_CONNECTION_TIMEOUT``
      - float
      - The maximum amount of time in seconds to wait for a TCP connection to be established.
    * - ``NEO4J_DRIVER_CONNECTION_ACQUISITION_TIMEOUT``
      - float
      - The maximum amount of time in seconds to wait for a connection to become available from the pool.
    * - ``NEO4J_DRIVER_CONNECTION_WRITE_TIMEOUT``
      - float
      - The maximum amount of time in seconds to wait for a TCP write operation to complete.
    * - ``NEO4J_DRIVER_MAX_CONNECTION_LIFETIME``
      - float
      - The maximum time in seconds a pooled connection can remain open before being closed.
    * - ``NEO4J_DRIVER_MAX_TRANSACTION_RETRY_TIME``
      - float
      - The maximum amount of time in seconds that a managed transaction will retry before failing.
    * - ``NEO4J_DRIVER_LIVENESS_CHECK_TIMEOUT``
      - float
      - The maximum amount of time in seconds to wait for a connection liveness check.
    * - ``NEO4J_DRIVER_KEEP_ALIVE``
      - bool
      - Specify whether TCP keep-alive should be enabled.
    * - ``NEO4J_DRIVER_ENCRYPTED``
      - bool
      - Specify whether to use an encrypted connection between the driver and server.
    * - ``NEO4J_DRIVER_USER_AGENT``
      - str
      - Specify the client agent name.
    * - ``NEO4J_DRIVER_NOTIFICATIONS_MIN_SEVERITY``
      - str
      - Set the minimum severity for notifications the server should send to the client (e.g., ``OFF``, ``WARNING``).
    * - ``NEO4J_DRIVER_NOTIFICATIONS_DISABLED_CATEGORIES``
      - list
      - A comma-separated list of notification categories to disable (e.g., ``DEPRECATION``, ``PERFORMANCE``).
    * - ``NEO4J_DRIVER_NOTIFICATIONS_DISABLED_CLASSIFICATIONS``
      - list
      - A comma-separated list of notification classifications to disable.
    * - ``NEO4J_DRIVER_WARN_NOTIFICATION_SEVERITY``
      - str
      - The severity level at which notifications should be logged as warnings by the driver.
    * - ``NEO4J_DRIVER_TELEMETRY_DISABLED``
      - bool
      - Specify whether to disable sending anonymous telemetry data to the server.

