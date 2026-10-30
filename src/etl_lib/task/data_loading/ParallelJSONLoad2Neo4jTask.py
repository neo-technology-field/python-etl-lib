import abc
import logging
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor
from etl_lib.core.SplittingBatchProcessor import (SplittingBatchProcessor,
                                                  dict_id_extractor)
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.JSONBatchSource import JSONBatchSource


class ParallelJSONLoad2Neo4jTask(Task):
    """
    Parallel JSON to Neo4j load using the mix-and-batch strategy.

    Wires a :py:class:`~etl_lib.data_source.JSONBatchSource.JSONBatchSource`, optional
    Pydantic validation, a diagonal splitter (to avoid overlapping node locks), and a
    Cypher sink. Records are distributed into (row, col) partitions and processed in
    non-overlapping groups, exactly as in
    :py:class:`~etl_lib.task.data_loading.ParallelCSVLoad2Neo4jTask.ParallelCSVLoad2Neo4jTask`.

    Unlike the CSV variant there are no reader kwargs to forward. ``JSONBatchSource``
    detects the JSON layout from the file content itself.

    Args:
        context: Shared ETL context.
        file: JSON file to load, either JSON Lines or an array, optionally gzipped.
        model: Optional Pydantic model for record validation. Invalid records go to `error_file`.
        error_file: Output for invalid records. Required when `model` is set.
        table_size: Bucketing grid size for the splitter.
        batch_size: Per-cell target batch size from the splitter.
        max_workers: Worker threads per wave.
        prefetch: Number of waves to prefetch from the splitter.

    Notes:
        - `_query()` must return Cypher that starts with ``UNWIND $batch AS row``.
        - Override `_id_extractor()` if your records do not expose ``start``/``end``. The
          default uses :py:func:`etl_lib.core.SplittingBatchProcessor.dict_id_extractor`.
        - Validation round-trips each record through the Pydantic model, so source-injected
          fields such as ``_row`` reach the query only when no model is given.
    """

    def __init__(self,
                 context: ETLContext,
                 file: Path,
                 model: Type[BaseModel] | None = None,
                 error_file: Path | None = None,
                 table_size: int = 10,
                 batch_size: int = 5000,
                 max_workers: int | None = None,
                 prefetch: int = 4):
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
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def run_internal(self, **kwargs) -> TaskReturn:
        json_source = JSONBatchSource(self.context, self, self.file)
        predecessor = json_source
        if self.model is not None:
            predecessor = ValidationBatchProcessor(self.context, self, json_source, self.model, self.error_file)

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
    def _query(self) -> str:
        pass
