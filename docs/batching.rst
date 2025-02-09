Batching and Streaming
======================

As outlined in :ref:`overview`, :class:`~etl_lib.core.Task.Task` and :class:`~etl_lib.core.Task.TaskGroup` let you compose pipelines. A typical task would be 'load a CSV file into the database'.

This is actually a series of steps, such as:

* read the CSV data
* validate each row
* import the rows into the database

Ideally, this would be done in batches of rows, to allow processing of large files without overusing memory.

In addition, there will be a lot of duplicated code for each implementation of such a task.

Therefore tasks can be composed of :class:`~etl_lib.core.BatchProcessor.BatchProcessor`.

The :func:`~etl_lib.core.BatchProcessor.BatchProcessor.__init__` constructor takes a predecessor instance. This predecessor will be used as source of data during calls to :func:`~etl_lib.core.BatchProcessor.BatchProcessor.get_batch`. This function returns a ``Generator`` of :class:`~etl_lib.core.BatchProcessor.BatchResults`, thus allowing to create chain of batch processing.
The last part in the chain starts the processing and sets the ``max_batch__size`` for the whole of the chain.

The :class:`~etl_lib.core.ClosedLoopBatchProcessor.ClosedLoopBatchProcessor` is a convenience implementation for this last step that also integrated the batching with the logging and reporting functionality.

For the above mentioned CSV loading task, see below figure:

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

An implementation of this can be found in :class:`~etl_lib.core.task.data_loading.CSVLoad2Neo4jTask.CSVLoad2Neo4jTask`::

    def run_internal(self, **kwargs) -> TaskReturn:
        error_file = self.file.with_suffix(".error.json")

        csv = CSVBatchProcessor(self.file, self.context, self)
        validator = ValidationBatchProcessor(self.context, self, csv, self.model, error_file)
        cypher = CypherBatchProcessor(self.context, self, validator, self._query())
        end = ClosedLoopBatchProcessor(self.context, self, cypher)
        result = next(end.get_batch(self.batch_size))

        return TaskReturn(True, result.statistics)


If validation is not needed, the ``validator`` can simple be removed or substituted with another implementation.

The library provides a few ``BatchProcessor`` implementation to build task from.

Splitting a task into sub steps also allows for simpler testing, as each step (``BatchProcessor``) can be tested in isolation.

Part of the ``BatchResults`` passed between the steps is a dictionary that contains information about the batches. The keys in this dictionary depend on the processors involved. The ``csv`` processor used above would add a ``csv_lines_read`` and the ``validator`` would add ``valid_rows`` and ``invalid_rows`` entries.

The  :class:`~etl_lib.core.ValidationBatchProcessor.ValidationBatchProcessor` at the end of the chain would aggregate this information and send it to the reporter. If reporting to the database is enabled, then each progressed batched will result in a update, allowing live monitoring.




