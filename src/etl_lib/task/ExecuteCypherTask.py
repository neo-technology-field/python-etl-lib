import abc

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.utils import merge_summery


class ExecuteCypherTask(Task):

    def __init__(self, context: ETLContext):
        super().__init__(context)
        self.context = context

    def run_internal(self, **kwargs) -> TaskReturn:
        with self.context.neo4j.session() as session:

            if isinstance(self._query(), list):
                stats = {}
                for query in self._query():
                    result = self.context.neo4j.query_database(session=session, query=query, **kwargs)
                    stats = merge_summery(stats, result.summery)
                return TaskReturn(True, stats)
            else:
                result = self.context.neo4j.query_database(session=session, query=self._query(), **kwargs)
                return TaskReturn(True, result.summery)

    @abc.abstractmethod
    def _query(self) -> str | list[str]:
        pass
