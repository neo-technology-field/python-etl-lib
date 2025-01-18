import abc
import logging
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchProcessor import CypherBatchProcessor
from etl_lib.data_source.CSVBatchProcessor import CSVBatchProcessor


class CSVLoad2Neo4jTasks(Task):

    def __init__(self, context: ETLContext, model: Type[BaseModel], file: Path, batch_size: int = 5000):
        super().__init__(context)
        self.batch_size = batch_size
        self.model = model
        self.logger = logging.getLogger(self.__class__.__name__)
        self.file = file
        self.addons["batch_size"] = batch_size

    def run_internal(self, **kwargs) -> TaskReturn:
        error_file = self.file.with_suffix(".error.json")

        csv = CSVBatchProcessor(self.file, self.context, self)
        validator = ValidationBatchProcessor(self.context, self, csv, self.model, error_file)
        cypher = CypherBatchProcessor(self.context, self, validator, self._query())
        end = ClosedLoopBatchProcessor(self.context, self, cypher)
        result = next(end.get_batch(self.batch_size))

        return TaskReturn(True, result.statistics)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file})"

    @abc.abstractmethod
    def _query(self):
        pass
