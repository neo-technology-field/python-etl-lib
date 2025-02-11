
.. toctree::
   :hidden:

   Documentation <self>
   batching
   reporting
   validation
   configuration
   testing
   cli
   API Reference <source/etl_lib>

Welcome to the neo4j-etl-lib documentation
==========================================

I often found myself implementing pipelines for small to medium-sized project and repeating the same code.
This library is an effort to collect building blocks in a way that simplify the process of assembling pipelines.

When building ETL pipelines the following criteria should be considered as a bare minimum:

* Logging (recording task execution, durations, errors, and statistics)
* Error handling
* Data validation (currently via Pydantic)
* Batching and streaming
* Optionally, tracking performed tasks and providing a way to review past ETL runs

.. note::

   This project is under active development, meaning that API changes may occur.

Installation
------------

The library can be installed via pip (inside a virtual environment):

.. code-block:: console

    pip install neo4j-etl-lib


..  _overview:

Overview
--------

The core of the library is the :class:`~etl_lib.core.Task.Task` class and its implementations.
This class represents an individual job to be executed.

Each task receives an instance of :class:`~etl_lib.core.ETLContext.ETLContext` during construction,
providing access to shared functionality such as database interactions.

Subclasses only need to implement the  :func:`~etl_lib.core.Task.Task.run_internal()` method.
The base task class handles other concerns, such as time tracking, reporting, and error handling.

Tasks can be grouped into :class:`~etl_lib.core.Task.TaskGroup` or :class:`~etl_lib.core.Task.ParallelTaskGroup`.
These groups help organize the pipeline, for example, by grouping all loading tasks or post-processing tasks.
This structure allows executing only specific parts of the pipeline when needed.

A pipeline is represented by a :class:`~etl_lib.core.Task.Task` or :class:`~etl_lib.core.Task.TaskGroup`,
which serves as the root of a tree of tasks and task groups.
To start the pipeline, simply call :func:`~etl_lib.core.Task.Task.execute` on the root task or task group.



