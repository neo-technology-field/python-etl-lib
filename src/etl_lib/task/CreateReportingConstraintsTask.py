from etl_lib.core.Task import Task, TaskReturn


class CreateReportingConstraintsTask(Task):
    """Creates the constraint in the REPORTER_DATABASE database."""

    def __init__(self, context):
        super().__init__(context)

    def run_internal(self, **kwargs) -> TaskReturn:
        database = self.context.env("REPORTER_DATABASE")
        assert database is not None, "REPORTER_DATABASE needs to be set in order to run this task"

        with self.context.neo4j.session(database) as session:
            result = self.context.neo4j.query_database(session=session,
                                                       query="CREATE CONSTRAINT IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE")
            return TaskReturn(True, result.summery)
