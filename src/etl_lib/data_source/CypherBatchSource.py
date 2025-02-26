from typing import Generator, Callable, Optional

from neo4j import Record

from etl_lib.core.BatchProcessor import BatchResults, BatchProcessor
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task


class CypherBatchSource(BatchProcessor):

    def __init__(
            self,
            context: ETLContext,
            task: Task,
            query: str,
            record_transformer: Optional[Callable[[Record], dict]] = None,
            **kwargs
    ):
        """
       Constructs a new CypherBatchSource.

       Args:
           context: :class:`etl_lib.core.ETLContext.ETLContext` instance.
           task: :class:`etl_lib.core.Task.Task` instance owning this batchProcessor.
           query: Cypher query to execute.
           record_transformer: Optional function to transform each record. See Neo4j API documentation on `result_transformer_`
           kwargs: Arguments passed as parameters with the query.
       """
        super().__init__(context, task)
        self.query = query
        self.record_transformer = record_transformer
        self.kwargs = kwargs

    def __read_records(self, tx, batch_size):
        batch_ = []
        result = tx.run(self.query, **self.kwargs)

        for record in result:
            data = record.data()
            if self.record_transformer:
                data = self.record_transformer(data)
            batch_.append(data)

            if len(batch_) == batch_size:
                yield batch_
                batch_ = []

        if batch_:
            yield batch_

    def get_batch(self, max_batch_size: int) -> Generator[BatchResults, None, None]:
        # not using managed tx on purpose. First of, we want to keep the tx open while delivering batches
        # automatic retry logic would help, as we do not want to start the query again
        with self.context.neo4j.session() as session:
            with session.begin_transaction() as tx:
                for chunk in self.__read_records(tx, max_batch_size):
                    yield BatchResults(
                        chunk=chunk,
                        statistics={"cypher_rows_read": len(chunk)},
                        batch_size=len(chunk)
                    )
