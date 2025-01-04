import abc
import logging
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.ETLContext import ETLContext
from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.Task import Task, InternalResult
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchProcessor import CypherBatchProcessor
from etl_lib.data_source.CSVBatchProcessor import CSVBatchProcessor


class CSVLoad2Neo4jTasks(Task):

    def __init__(self, context: ETLContext, model: Type[BaseModel], batch_size: int = 5000, log_indent: int = 1):
        super().__init__(context, log_indent)
        self.batch_size = batch_size
        self.model = model
        self.logger = logging.getLogger(self.__class__.__name__)

    def run_internal(self, file: Path, **kwargs) -> InternalResult:

        error_file = file.with_suffix(".error.json")

        csv = CSVBatchProcessor(file)
        validator = ValidationBatchProcessor(csv, self.model, error_file)
        cypher = CypherBatchProcessor(validator, self.context.neo4j, self._query())
        end = ClosedLoopBatchProcessor(cypher)
        result = next(end.get_batch(self.batch_size))

        return InternalResult(True, result.statistics)


    @abc.abstractmethod
    def _query(self):
        pass
