..
   Note: Items in this toctree form the top-level navigation. See `api.rst` for the `autosummary` directive, and for why `api.rst` isn't called directly.

.. toctree::
   :hidden:

   Documentation <self>
   batching
   reporting
   cli
   API Reference <source/etl_lib>

Welcome to the neo4j-etl-lib documentation
==========================================

I often found myself implementing pipelines for small to medium sized project and repeating the same code again and again.
This library is an effort to collect building blocks in a way that makes assembling pipelines out of these building blocks easy.

When building ETL pipelines the following criteria should be considered as a bare minimum:

* logging (of tasks performed including times, errors, and statistics)
* error handling
* validation of data (currently via Pydantic)
* batching and streaming
* optionally record the information about performed tasks and provide means to review past etl runs.

.. note::

   This project is under active development, meaning changes in the API are possible.

Installation
------------

The library can be installed via pip (inside a venv):

.. code-block:: console

    pip install neo4j-etl-lib


..  _overview:

Overview
--------

The library is centered around the :class:`~etl_lib.core.Task.Task` class and its implementations.
This class represents a job that is to be executed.

This class gets a reference to an :class:`~etl_lib.core.ETLContext.ETLContext` instance during construction.
From there, the task has access to other functionality, such as database interactions.

Sub classes should only need to implement the :func:`~etl_lib.core.Task.Task.run_internal()` member function.
The task takes care of the other needs, such as time tacking, reporting and error handling.

Tasks can be grouped together into :class:`~etl_lib.core.Task.TaskGroup` or :class:`~etl_lib.core.Task.ParallelTaskGroup`.
Such groups help in organising the pipeline, for instance by grouping all loading tasks or all post-processing task into groups. This helps in executing only parts of the pipeline if needed.

A pipeline is represented by a TaskGroup, beeing the root of a tree of other Tasks or TaskGroups.
Starting the pipeline is then simply the call to :func:`~etl_lib.core.Task.Task.execute()` on the root task(group).


