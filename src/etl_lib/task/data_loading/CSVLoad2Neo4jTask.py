import abc
import logging
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.CSVBatchSource import CSVBatchSource


class CSVLoad2Neo4jTask(Task):
    """
    Loads the specified CSV file to Neo4j.

    Uses BatchProcessors to read, validate and write to Neo4j.
    The validation step is using pydantic, hence a Pydantic model needs to be provided.
    Rows that fail the validation, will be written to en error file. The location of the error file is determined as
    follows:

    If the context env vars hold an entry `ETL_ERROR_PATH` the file will be place there, with the name set to name
    of the provided filename appended with `.error.json`

    If  `ETL_ERROR_PATH` is not set, the file will be placed in the same directory as the CSV file.
    """
    def __init__(self, context: ETLContext, model: Type[BaseModel], file: Path, batch_size: int = 5000):
        super().__init__(context)
        self.batch_size = batch_size
        self.model = model
        self.logger = logging.getLogger(self.__class__.__name__)
        self.file = file

    def run_internal(self, **kwargs) -> TaskReturn:
        error_path = self.context.env("ETL_ERROR_PATH")
        if error_path is None:
            error_file = self.file.with_suffix(".error.json")
        else:
            error_file = error_path / self.file.with_name(self.file.stem + ".error.json").name

        csv = CSVBatchSource(self.file, self.context, self)
        validator = ValidationBatchProcessor(self.context, self, csv, self.model, error_file)
        cypher = CypherBatchSink(self.context, self, validator, self._query())
        end = ClosedLoopBatchProcessor(self.context, self, cypher)
        result = next(end.get_batch(self.batch_size))

        return TaskReturn(True, result.statistics)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file})"

    @abc.abstractmethod
    def _query(self):
        pass
