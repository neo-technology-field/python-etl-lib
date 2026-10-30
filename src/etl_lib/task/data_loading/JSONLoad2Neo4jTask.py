import abc
import logging
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.ValidationBatchProcessor import ValidationBatchProcessor
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.JSONBatchSource import JSONBatchSource


class JSONLoad2Neo4jTask(Task):
    """
    Loads the specified JSON file to Neo4j.

    See :doc:`/concepts` for the conventions a load task follows.

    Accepts both layouts supported by
    :py:class:`~etl_lib.data_source.JSONBatchSource.JSONBatchSource`: JSON Lines and a
    single JSON array of objects. Array input requires the optional ``ijson`` dependency.

    The pipeline mirrors
    :py:class:`~etl_lib.task.data_loading.CSVLoad2Neo4jTask.CSVLoad2Neo4jTask`: read in
    batches, optionally validate each record against a Pydantic model, and write to Neo4j
    via the subclass-provided Cypher query. Records failing validation go to an error file
    located exactly as in the CSV task: under the ``ETL_ERROR_PATH`` context env var when
    set, otherwise next to the input file with ``.error.json`` appended.

    Subclasses must implement :py:meth:`_query`, returning Cypher that starts with
    ``UNWIND $batch AS row``.

    Validation round-trips each record through the Pydantic model. Source-injected fields
    such as the ``_row`` field added by
    :py:class:`~etl_lib.data_source.JSONBatchSource.JSONBatchSource` therefore reach the
    query only when no model is given. With a model, ``row._row`` is absent and Cypher
    yields ``null`` for it rather than raising.
    """

    def __init__(self,
                 context: ETLContext,
                 file: Path,
                 model: Type[BaseModel] | None = None,
                 batch_size: int = 5000):
        super().__init__(context)
        self.batch_size = batch_size
        self.model = model
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self.file = file

    def run_internal(self, **kwargs) -> TaskReturn:
        json_source = JSONBatchSource(self.context, self, self.file)
        predecessor = json_source

        if self.model is not None:
            error_path = self.context.env("ETL_ERROR_PATH")
            if error_path is None:
                error_file = self.file.with_suffix(".error.json")
            else:
                error_file = error_path / self.file.with_name(self.file.stem + ".error.json").name

            predecessor = ValidationBatchProcessor(self.context, self, json_source, self.model, error_file)

        cypher = CypherBatchSink(self.context, self, predecessor, self._query())
        end = ClosedLoopBatchProcessor(self.context, self, cypher)
        result = next(end.get_batch(self.batch_size))

        return TaskReturn(True, result.statistics)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.file})"

    @abc.abstractmethod
    def _query(self) -> str:
        pass
