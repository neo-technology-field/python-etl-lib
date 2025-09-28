from abc import ABC, abstractmethod
from typing import Callable, Union

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor
from etl_lib.core.SplittingBatchProcessor import SplittingBatchProcessor, dict_id_extractor
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.SQLBatchSource import SQLBatchSource
from sqlalchemy import text


class ParallelSQLLoad2Neo4jTask(Task, ABC):
    """
    Parallelized version of SQLLoad2Neo4jTask: reads via SQLBatchSource,
    splits into non-overlapping partitions (grid), processes each partition
    in parallel through a CypherBatchSink, and closes the loop.

    Subclasses must implement:
        - _sql_query()
        - _cypher_query()
        - optionally override _count_query() and _id_extractor().

    Control parameters:
        batch_size: max items per partition batch
        table_size: dimension of the splitting grid
        max_workers: parallel threads per partition group (defaults to table_size)
        prefetch: number of partition-groups to prefetch
    """

    def __init__(
            self,
            context: ETLContext,
            batch_size: int = 5000,
            table_size: int = 10,
            max_workers: int = None,
            prefetch: int = 4
    ):
        super().__init__(context)
        self.context = context
        self.batch_size = batch_size
        self.table_size = table_size
        # default max_workers to table_size for full parallelism
        self.max_workers = max_workers or table_size
        self.prefetch = prefetch

    @abstractmethod
    def _sql_query(self) -> str:
        """
        Return the SQL query to load source rows.
        """
        pass

    @abstractmethod
    def _cypher_query(self) -> str:
        """
        Return the Cypher query to write rows into Neo4j.
        """
        pass

    def _count_query(self) -> Union[str, None]:
        """
        Optional SQL to count source rows for progress reporting.
        """
        return None

    def _id_extractor(self) -> Callable:
        """
        Extractor mapping each row item to a (row,col) partition index.
        Default expects dict rows with 'start' and 'end' keys.
        Override to customize.
        """
        return dict_id_extractor()

    def run_internal(self) -> TaskReturn:
        # total count for ClosedLoopBatchProcessor
        total_count = self.__get_source_count()
        # source of raw rows
        source = SQLBatchSource(self.context, self, self._sql_query())

        # splitter: non-overlapping partitions as defined by the id_extractor
        splitter = SplittingBatchProcessor(
            context=self.context,
            task=self,
            predecessor=source,
            table_size=self.table_size,
            id_extractor=self._id_extractor()
        )

        # parallel processor: runs CypherBatchSink on each partition concurrently
        parallel = ParallelBatchProcessor(
            context=self.context,
            task=self,
            worker_factory=lambda: CypherBatchSink(context=self.context, task=self, predecessor=None,
                                                   query=self._cypher_query()),
            predecessor=splitter,
            max_workers=self.max_workers,
            prefetch=self.prefetch
        )

        # close loop: drives the pipeline and reports progress
        closing = ClosedLoopBatchProcessor(
            context=self.context,
            task=self,
            predecessor=parallel,
            expected_rows=total_count
        )

        # run once to completion and return aggregated stats
        result = next(closing.get_batch(self.batch_size))
        return TaskReturn(True, result.statistics)

    def __get_source_count(self):
        count_query = self._count_query()
        if count_query is None:
            return None
        with self.context.sql.engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(count_query))
                row = result.fetchone()
                return row[0] if row else None
