from typing import Generator

from etl_lib.ETLContext import Neo4jContext
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults, append_result


class CypherBatchProcessor(BatchProcessor):

    def __init__(self, predecessor: BatchProcessor, neo4j_context: Neo4jContext, query: str):
        super().__init__(predecessor)
        self.query = query
        self.neo4j = neo4j_context

    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        assert self.predecessor is not None

        with self.neo4j.session() as session:
            for batch_result in self.predecessor.get_batch(batch_size):
                result = self.neo4j.query_database(session=session, query=self.query, batch=batch_result.chunk)
                yield append_result(batch_result, result.summery)
