Batching and Streaming
======================

As outlined in :ref:`overview`, :class:`~etl_lib.core.Task.Task` and :class:`~etl_lib.core.Task.TaskGroup` let you compose pipelines. A typical task would be 'load a CSV file into the database'.

This is actually a series of steps, such as:

* Read the CSV data
* Validate each row
* Import the rows into the database

Ideally, this would be done in batches of rows to allow processing of large files without excessive memory usage.

Additionally, there would be a lot of duplicated code for each implementation of such a task.

To address this, tasks can be composed using :class:`~etl_lib.core.BatchProcessor.BatchProcessor`.

The :func:`~etl_lib.core.BatchProcessor.BatchProcessor.__init__` constructor takes a predecessor instance. This predecessor serves as the data source during calls to :func:`~etl_lib.core.BatchProcessor.BatchProcessor.get_batch`. This function returns a ``Generator`` of :class:`~etl_lib.core.BatchProcessor.BatchResults`, enabling the creation of a chain of batch processors.
The final component in the chain initiates the processing and sets the ``max_batch_size`` for the entire chain.

The :class:`~etl_lib.core.ClosedLoopBatchProcessor.ClosedLoopBatchProcessor` is a convenient implementation for this final step, integrating batching with logging and reporting functionality.

For the CSV loading task mentioned above, see the figure below:

.. graphviz::

   digraph example {
        rankdir=RL;
        node [ shape=record ];

        csv [
            label = "csv|<get_batch>get_batch";
        ];

        validator [
            label = "validator|<get_batch>get_batch";
        ];

        cypher [
            label = "cypher|<get_batch>get_batch";
        ];

        end [
            label = "end|<get_batch>get_batch";
        ];

        end:get_batch -> cypher:get_batch -> validator:get_batch -> csv:get_batch;
   }

An implementation of this can be found in :class:`~etl_lib.task.data_loading.CSVLoad2Neo4jTask.CSVLoad2Neo4jTask`::

    def run_internal(self, **kwargs) -> TaskReturn:
        error_file = self.file.with_suffix(".error.json")

        csv = CSVBatchProcessor(self.file, self.context, self)
        validator = ValidationBatchProcessor(self.context, self, csv, self.model, error_file)
        cypher = CypherBatchProcessor(self.context, self, validator, self._query())
        end = ClosedLoopBatchProcessor(self.context, self, cypher)
        result = next(end.get_batch(self.batch_size))

        return TaskReturn(True, result.statistics)

If validation is not needed, the ``validator`` can simply be removed or substituted with another implementation.

The library provides several ``BatchProcessor`` implementations to build tasks from.

Splitting a task into sub-steps also simplifies testing, as each step (``BatchProcessor``) can be tested in isolation.

A dictionary containing batch metadata is passed between steps via ``BatchResults``. The keys in this dictionary depend on the processors involved. For instance, the ``csv`` processor used above would add a ``csv_lines_read`` entry, while the ``validator`` would add ``valid_rows`` and ``invalid_rows`` entries.

The :class:`~etl_lib.core.ClosedLoopBatchProcessor.ClosedLoopBatchProcessor`, at the end of the chain, aggregates this information and sends it to the reporter. If database reporting is enabled, each processed batch will trigger an update, allowing real-time monitoring.
