import abc
from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor
from etl_lib.core.SplittingBatchProcessor import (SplittingBatchProcessor,
                                                  dict_id_extractor)
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.ParquetBatchSource import ParquetBatchSource


class ParallelParquetLoad2Neo4jTask(Task):
    """
    Parallel Parquet → Neo4j load using the mix-and-batch strategy.
    """
    def __init__(self,
                 context: ETLContext,
                 file: Path,
                 model: Optional[Type[BaseModel]] = None,
                 error_file: Optional[Path] = None,
                 table_size: int = 10,
                 batch_size: int = 5000,
                 max_workers: Optional[int] = None,
                 prefetch: int = 4,
                 **parquet_reader_kwargs):
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
        self.parquet_reader_kwargs = parquet_reader_kwargs

    def run_internal(self) -> TaskReturn:
        total_count = ParquetBatchSource.get_total_rows(self.file)

        source = ParquetBatchSource(self.file, self.context, self, **self.parquet_reader_kwargs)
        
        predecessor = source
        if self.model is not None:
            predecessor = ValidationBatchProcessor(self.context, self, source, self.model, self.error_file)

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

        closing = ClosedLoopBatchProcessor(self.context, self, parallel, expected_rows=total_count)
        result = next(closing.get_batch(self.batch_size))
        return TaskReturn(True, result.statistics)

    def _id_extractor(self):
        return dict_id_extractor()

    @abc.abstractmethod
    def _query(self):
        pass
