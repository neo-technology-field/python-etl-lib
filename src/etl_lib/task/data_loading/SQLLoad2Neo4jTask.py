from abc import abstractmethod

from etl_lib.core.ETLContext import ETLContext
from sqlalchemy import text

from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_source.SQLBatchSource import SQLBatchSource


class SQLLoad2Neo4jTask(Task):
    '''
    Load the output of the specified SQL query to Neo4j.

    Uses BatchProcessors to read and write data.
    Subclasses must implement the methods returning the SQL and Cypher queries.

    Example usage: (from the MusicBrainz example)

    .. code-block:: python


        class LoadArtistCreditTask(SQLLoad2Neo4jTask):
            def _sql_query(self) -> str:
                return """
                    SELECT ac.id AS artist_credit_id, ac.name AS credit_name
                    FROM artist_credit ac;
                    """

            def _cypher_query(self) -> str:
                return """
                       UNWIND $batch AS row
                       MERGE (ac:ArtistCredit {id: row.artist_credit_id})
                       SET ac.name = row.credit_name
                      """

            def _count_query(self) -> str | None:
                return "SELECT COUNT(*) FROM artist_credit;"

    '''

    def __init__(self, context: ETLContext, batch_size: int = 5000):
        super().__init__(context)
        self.context = context
        self.batch_size = batch_size

    @abstractmethod
    def _sql_query(self) -> str:
        """
        Return the SQL query to load the source data.
        """
        pass

    @abstractmethod
    def _cypher_query(self) -> str:
        """
        Return the Cypher query to write the data in batches to Neo4j.
        """
        pass

    def _count_query(self) -> str | None:
        """
        Return the SQL query to count the number of rows returned from :func:`_sql_query`.

        Optional. If provided, it will run once at the beginning of the task and
        provide the :class:`etl_lib.core.ClosedLoopBatchProcessor` with the total number of rows.
        """
        return None

    def run_internal(self) -> TaskReturn:
        total_count = self.__get_source_count()
        source = SQLBatchSource(self.context, self, self._sql_query())
        sink = CypherBatchSink(self.context, self, source, self._cypher_query())

        end = ClosedLoopBatchProcessor(self.context, self, sink, total_count)

        result = next(end.get_batch(self.batch_size))
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
