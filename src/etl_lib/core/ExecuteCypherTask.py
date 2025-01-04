import abc

from etl_lib.ETLContext import ETLContext
from etl_lib.core.Task import Task, InternalResult, merge_summery


class ExecuteCypherTask(Task):

    def __init__(self, context: ETLContext, log_indent: int = 1):
        super().__init__(context, log_indent)
        self.context = context

    def run_internal(self, **kwargs) -> InternalResult:
        with self.context.neo4j.session() as session:

            if isinstance(self._query(), list):
                stats = {}
                for query in self._query():
                    result = self.context.neo4j.query_database(session=session, query=query, **kwargs)
                    stats = merge_summery(stats, result.summery)
                return InternalResult(True, stats)
            else:
                result = self.context.neo4j.query_database(session=session, query=self._query(), **kwargs)
                return InternalResult(True, result.summery)

    @abc.abstractmethod
    def _query(self) -> str | list[str]:
        pass
