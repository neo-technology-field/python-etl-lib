..
   Note: Items in this toctree form the top-level navigation. See `api.rst` for the `autosummary` directive, and for why `api.rst` isn't called directly.

.. toctree::
   :hidden:

   Home page <self>
   API reference <_autosummary/etl_lib>

Welcome to the Neo4j-etl-lib documentation
==========================================

While there are already many ETL solutions available, for small to medium sized project, the effort to set them up is not always justified, or company politics prohibit using them.

When building ETL pipelines the following criteria should be considered as a bare minimum:

* logging (of tasks performed including times, errors, and statistics)
* error handling
* validation of data (currently via Pydantic)
* batching and streaming
* optionally record the information about performed tasks and provide means to review past etl runs.

Instead of building each of these from scratch each time, this package aims to provide reusable components to quickly assemble pipelines.

