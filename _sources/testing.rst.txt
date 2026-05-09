Testing
=======

Tests for ETL pipelines are, by definition, integration tests. The library provides utilities to simplify setting up a connection to Neo4j.

The utilities integrate with ``pytest``. See the provided examples for possible setup and usage.

There are two ways to use Neo4j with tests: either with the excellent `TestContainers <https://testcontainers-python.readthedocs.io/en/latest/>`_ or by using an existing Neo4j installation.

The module :mod:`~etl_lib.test_utils.utils` contains mock implementations of core components to set up an ETLContext and its dependencies. These utilities are designed to integrate seamlessly and use environment variables for configuration.

Fixtures
--------

In addition to the above module, the following code is recommended to be placed in the root of the testing package as ``conftest.py``. The ``pytest`` fixtures defined within it will be available to all tests.

.. literalinclude:: ../examples/gtfs/tests/conftest.py
   :language: python
   :linenos:
   :start-after: # DOC_INCLUDE_START_HERE
   :end-before: # DOC_INCLUDE_END_HERE

The following fixtures are provided:

* ``neo4j_driver_with_empty_db`` : Provides a Neo4j driver object connected to an empty database. Typically used in data setup steps.
* ``neo4j_driver`` : Provides a Neo4j driver object connected to a running Neo4j installation.
* ``etl_context`` : Provides a :class:`~etl_lib.core.ETLContext.ETLContext` for testing purposes. The simple reporter is used, and a ``pytest`` temporary directory is set up for error files (:doc:`validation`).

Selecting a Neo4j Installation
------------------------------

Testcontainers
--------------

If the ``NEO4J_TEST_CONTAINER`` environment variable is set to the name of a Neo4j Docker image, TestContainers will be used to run Neo4j. This requires Docker to be available on the host running the tests.

The provided installation will have apoc core and gds plugins installed.

External Installation
---------------------

If the ``NEO4J_TEST_CONTAINER`` environment variable is not set, the ``NEO4J_URI``, ``NEO4J_USERNAME``, and ``NEO4J_PASSWORD`` variables must be set. These will be used to connect to an existing Neo4j instance.

The functions in :mod:`~etl_lib.test_utils.utils` respect the ``NEO4J_TEST_DATABASE`` variable when running queries, allowing for separate databases per user.
