import abc

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task, TaskReturn
from etl_lib.core.utils import merge_summary


class ExecuteCypherTask(Task):
    """
    Execute Cypher against the configured Neo4j database as a Task.

    This task is for data refinement and schema work, so query results are not returned.
    Subclasses implement ``_query()`` and return either one Cypher string or a list of
    strings. A list runs in order through a single session and its statistics counters are
    summed into the execution summary.

    Each statement goes through its own managed transaction, so a failing statement does
    not roll back the ones before it. Use a single Cypher statement when the work must be
    atomic.

    Query parameters are supplied to ``execute()`` or ``run_internal()`` as keyword
    arguments, for example ``task.execute(id=1)``, and are bound as ``$id`` in the Cypher.
    They are not constructor arguments.

    Args:
        context: The ETL context providing the Neo4j connection.
    """

    def __init__(self, context: ETLContext):
        super().__init__(context)
        self.context = context

    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Execute the statements returned by ``_query()``.

        The hook is evaluated once, before the session is opened, so a generated statement
        list is not recomputed per statement or per execution step.

        Args:
            **kwargs: Query parameters forwarded to every statement.

        Returns:
            A successful task result with the summed statistics counters. An empty
            statement list produces an empty summary.

        Raises:
            neo4j.exceptions.Neo4jError: If Neo4j rejects a statement or the write
                transaction cannot be completed.
            Exception: Whatever the ``_query()`` hook raises propagates here unchanged.
                Subclasses that load their statements in the hook, such as those built by
                :py:meth:`~etl_lib.task.SchemaTask.SchemaTask.from_file`, raise their
                file and parsing errors from this method rather than at construction.
                :py:meth:`~etl_lib.core.Task.Task.execute` records them on an
                unsuccessful task result.
        """
        queries = self._query()
        if isinstance(queries, str):
            queries = [queries]
        statistics = {}
        with self.context.neo4j.session() as session:
            for query in queries:
                result = self.context.neo4j.query_database(session=session, query=query, **kwargs)
                statistics = merge_summary(statistics, result.summary)
        return TaskReturn(success=True, summary=statistics)

    @abc.abstractmethod
    def _query(self) -> str | list[str]:
        """
        Return the Cypher to execute.

        Returns:
            A single statement, or a list of statements run in the given order.
        """
        pass
