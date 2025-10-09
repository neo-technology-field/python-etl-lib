Parallel Processing
===================

To speed up processing, certain tasks can be executed in parallel. Care must be taken when writing from multiple threads to a database to avoid locking issues. The speedup achieved by parallel processing depends on many factors, such as I/O on the database, number of available cores, network latency, and so on.

Two options are available for parallel processing of data:

ParallelTaskGroup
-----------------

Similar to :class:`~etl_lib.core.Task.TaskGroup`, the class :class:`~etl_lib.core.Task.ParallelTaskGroup` allows grouping :class:`~etl_lib.core.Task.Task` together.

While :class:`~etl_lib.core.Task.TaskGroup` executes the assigned tasks sequentially and finishes when the last task in the chain is finished, the :class:`~etl_lib.core.Task.ParallelTaskGroup` runs them in parallel and waits for all contained tasks to finish before aggregating statistics and finishing itself.

This is an easy way to parallelize when the tasks are not touching the same part of the graph.

Mix and Batch
-------------

This approach especially helps when loading relationships. It follows the `Mix and batch technique <https://neo4j.com/blog/developer/mix-and-batch-relationship-load/>`__ by splitting the incoming data into non-overlapping batches and processing these batches in parallel.

For this, the :class:`~etl_lib.core.SplittingBatchProcessor.SplittingBatchProcessor` splits incoming batches into non-overlapping groups using a provided ID extractor. The :class:`~etl_lib.core.ParallelBatchProcessor.ParallelBatchProcessor` then processes these batches in parallel.

ID extractors
^^^^^^^^^^^^^
Two extractors are provided:

* :func:`~etl_lib.core.SplittingBatchProcessor.tuple_id_extractor` extracts IDs from a tuple
* :func:`~etl_lib.core.SplittingBatchProcessor.dict_id_extractor` extracts IDs from a dict / map

Custom extractors can be supplied. Range validation (0 <= row,col < table_size) is performed by the splitter.

Parameters that influence parallelism
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* `table_size` — size of the bucketing grid and number of diagonal partitions. Also acts as an upper bound for parallelism (often set equal to max_workers).
* `batch_size` — per-cell target size. A group is emitted during streaming only when all cells in that group have at least batch_size items. On source exhaustion, leftovers are flushed in waves that still respect batch_size per cell.
* `burst_factor` — to avoid unbounded buffers, if any cell in a group grows beyond a backlog threshold (default 20 × batch_size), that group is flushed early, capped at batch_size per cell. The threshold is configurable via SplittingBatchProcessor.burst_factor.
* `max_workers` — number of worker threads used by the parallel processor. One worker processes one partition.
* `prefetch` — number of groups prefetched from the splitter to keep workers busy.

Processor roles
^^^^^^^^^^^^^^^

* :class:`~etl_lib.core.SplittingBatchProcessor.SplittingBatchProcessor` — partitions items by (row, col) and emits diagonal groups that do not overlap.
* :class:`~etl_lib.core.ParallelBatchProcessor.ParallelBatchProcessor` — runs one worker per partition and merges their results fail-fast.

Statistics and progress
^^^^^^^^^^^^^^^^^^^^^^^

The splitter emits empty statistics for interim waves; only the final wave carries the accumulated upstream statistics to avoid double counting.

The parallel processor can report progress after each partition with incremental stats and tick the batch count after each wave.

Two tasks implement these processors:

* :class:`~etl_lib.task.data_loading.ParallelCSVLoad2Neo4jTask.ParallelCSVLoad2Neo4jTask` to execute Cypher with data from a CSV source.
* :class:`~etl_lib.task.data_loading.ParallelSQLLoad2Neo4jTask.ParallelSQLLoad2Neo4jTask` to execute Cypher with data from a SQL source.


For an usage example for the mix and batch technique see https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/nyc-taxi
