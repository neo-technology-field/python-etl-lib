import abc
from pathlib import Path
from typing import Type


from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor
from etl_lib.core.SplittingBatchProcessor import SplittingBatchProcessor, dict_id_extractor
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.CSVBatchSource import CSVBatchSource
from pydantic import BaseModel

class ParallelCSVLoad2Neo4jTask(Task):
    """
    Parallel CSV → Neo4j load using the mix-and-batch strategy.

    Wires a CSV reader, optional Pydantic validation, a diagonal splitter
    (to avoid overlapping node locks), and a Cypher sink. Rows are
    distributed into (row, col) partitions and processed in non-overlapping groups.

    Args:
        context: Shared ETL context.
        file: CSV file to load.
        model: Optional Pydantic model for row validation; invalid rows go to `error_file`.
        error_file: Output for invalid rows. Required when `model` is set.
        table_size: Bucketing grid size for the splitter.
        batch_size: Per-cell target batch size from the splitter.
        max_workers: Worker threads per wave.
        prefetch: Number of waves to prefetch from the splitter.
        **csv_reader_kwargs: Forwarded to :py:class:`etl_lib.data_source.CSVBatchSource.CSVBatchSource`.

    Returns:
        :py:class:`~etl_lib.core.Task.TaskReturn` with merged validation and Neo4j counters.

    Notes:
        - `_query()` must return Cypher that starts with ``UNWIND $batch AS row``.
        - Override `_id_extractor()` if your CSV schema doesn’t expose ``start``/``end``; the default uses
          :py:func:`etl_lib.core.SplittingBatchProcessor.dict_id_extractor`.
        - See the nyc-taxi example for a working subclass.
    """
    def __init__(self,
                 context: ETLContext,
                 file: Path,
                 model: Type[BaseModel] | None = None,
                 error_file: Path | None = None,
                 table_size: int = 10,
                 batch_size: int = 5000,
                 max_workers: int | None = None,
                 prefetch: int = 4,
                 **csv_reader_kwargs):
        super().__init__(context)
        self.file = file
        self.model = model
        if model is not None and error_file is None:
            raise ValueError('you must provide error file if the model is specified')
        self.error_file = error_file
        self.table_size = table_size
        self.batch_size = batch_size
        self.max_workers = max_workers or table_size
        self.prefetch = prefetch
        self.csv_reader_kwargs = csv_reader_kwargs

    def run_internal(self) -> TaskReturn:
        csv = CSVBatchSource(self.file, self.context, self, **self.csv_reader_kwargs)
        predecessor = csv
        if self.model is not None:
            predecessor = ValidationBatchProcessor(self.context, self, csv, self.model, self.error_file)

        splitter = SplittingBatchProcessor(
            context=self.context,
            task=self,
            predecessor=predecessor,
            table_size=self.table_size,
            id_extractor=self._id_extractor()
        )

        parallel = ParallelBatchProcessor(
            context=self.context,
            task=self,
            predecessor=splitter,
            worker_factory=lambda: CypherBatchSink(self.context, self, None, self._query()),
            max_workers=self.max_workers,
            prefetch=self.prefetch
        )

        closing = ClosedLoopBatchProcessor(self.context, self, parallel)
        result = next(closing.get_batch(self.batch_size))
        return TaskReturn(True, result.statistics)

    def _id_extractor(self):
        return dict_id_extractor()

    @abc.abstractmethod
    def _query(self):
        pass
