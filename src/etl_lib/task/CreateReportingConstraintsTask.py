from etl_lib.core.Task import Task, TaskReturn


class CreateReportingConstraintsTask(Task):
    """Creates the constraint in the REPORTER_DATABASE database."""

    def __init__(self, context):
        super().__init__(context)

    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Creates the unique constraint for ``ETLTask`` nodes in the reporting database.

        Raises:
            RuntimeError: If ``REPORTER_DATABASE`` is not configured. This is an explicit
                raise rather than an ``assert`` so the check also applies under ``python -O``.
        """
        database = self.context.env("REPORTER_DATABASE")
        if database is None:
            raise RuntimeError("REPORTER_DATABASE needs to be set in order to run this task")

        with self.context.neo4j.session(database) as session:
            result = self.context.neo4j.query_database(
                session=session, query="CREATE CONSTRAINT IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE"
            )
            return TaskReturn(True, result.summary)
