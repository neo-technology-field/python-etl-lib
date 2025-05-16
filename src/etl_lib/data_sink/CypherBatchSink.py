from typing import Generator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result
from etl_lib.core.Task import Task


class CypherBatchSink(BatchProcessor):
    """
    BatchProcessor to write batches of data to a Neo4j database.
    """

    def __init__(self, context: ETLContext, task: Task, predecessor: BatchProcessor, query: str, **kwargs):
        """
        Constructs a new CypherBatchSink.

        Args:
            context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
            task: :class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
            predecessor: BatchProcessor which :func:`~get_batch` function will be called to receive batches to process.
            query: Cypher to write the query to Neo4j.
                Data will be passed as `batch` parameter.
                Therefore, the query should start with a `UNWIND $batch AS row`.
        """
        super().__init__(context, task, predecessor)
        self.query = query
        self.neo4j = context.neo4j
        self.kwargs = kwargs

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        with self.neo4j.session() as session:
            for batch_result in self.predecessor.get_batch(batch_size):
                result = self.neo4j.query_database(session=session, query=self.query, batch=batch_result.chunk, **self.kwargs)
                yield append_result(batch_result, result.summery)
