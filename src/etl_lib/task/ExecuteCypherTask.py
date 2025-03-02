import abc

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.utils import merge_summery


class ExecuteCypherTask(Task):
    """
    Execute cypher (write) as a Task.

    This task is for data refinement jobs, as it does not return cypher results.
    Parameters can be passed as keyword arguments to the constructor and will be available as parameters inside cypher.
    """
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
                return TaskReturn(success=True, summery=stats)
            else:
                result = self.context.neo4j.query_database(session=session, query=self._query(), **kwargs)
                return TaskReturn(success=True, summery=result.summery)

    @abc.abstractmethod
    def _query(self) -> str | list[str]:
        pass
