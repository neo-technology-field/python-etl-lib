from abc import abstractmethod
from pathlib import Path
from typing import Optional, Type

from pydantic import BaseModel

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.ParquetBatchSource import ParquetBatchSource


class ParquetLoad2Neo4jTask(Task):
    """
    Load the output of a Parquet file to Neo4j sequentially.

    Uses BatchProcessors to read and write data.
    """

    def __init__(self, 
                 context: ETLContext, 
                 file: Path, 
                 model: Optional[Type[BaseModel]] = None,
                 error_file: Optional[Path] = None,
                 batch_size: int = 5000):
        super().__init__(context)
        self.file = file
        self.model = model
        if model is not None and error_file is None:
            raise ValueError('you must provide error file if the model is specified')
        self.error_file = error_file
        self.batch_size = batch_size

    @abstractmethod
    def _cypher_query(self) -> str:
        """
        Return the Cypher query to write the data in batches to Neo4j.
        Must start with UNWIND $batch as row
        """
        pass

    def run_internal(self) -> TaskReturn:
        total_count = ParquetBatchSource.get_total_rows(self.file)

        source = ParquetBatchSource(self.file, self.context, self)
        
        predecessor = source
        if self.model:
            predecessor = ValidationBatchProcessor(self.context, self, source, self.model, self.error_file)

        sink = CypherBatchSink(self.context, self, predecessor, self._cypher_query())

        end = ClosedLoopBatchProcessor(self.context, self, sink, total_count)

        result = next(end.get_batch(self.batch_size))
        return TaskReturn(True, result.statistics)
