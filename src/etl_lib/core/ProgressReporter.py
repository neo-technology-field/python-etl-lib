import logging
from datetime import datetime

from tabulate import tabulate

from etl_lib.core.Task import Task, TaskGroup, TaskReturn
from etl_lib.core.utils import add_sigint_handler


class ProgressReporter:
    """
    Responsible for reporting progress of :py:class:`etl_lib.core.Task` .

    This specific implementation uses the python logging module to log progress.
    Non-error logging is using the INFO level.
    """
    start_time: datetime
    end_time: datetime

    def __init__(self, context):
        self.context = context
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    def register_tasks(self, main: Task):
        """
        Registers a :py:class:`etl_lib.core.Task` with this reporter.

        Needs to be called once with the root task. The function will walk the tree of tasks and register them in turn.

        Args:
            main: Root of the task tree.
        """
        self.logger.info("\n" + self.__print_tree(main))

    def started_task(self, task: Task) -> Task:
        """
        Marks the task as started.

        Start the time keeping for this task and performs logging.

        Args:
            task: Task to be marked as started.

        Returns:
            The task that was provided.
        """
        task.start_time = datetime.now()
        self.logger.info(f"{'\t' * task.depth}starting {task.task_name()}")
        return task

    def finished_task(self, task: Task, result: TaskReturn) -> Task:
        """
        Marks the task as finished.

        Stops the time recording for the tasks and performs logging. Logging will include details from the provided summery.

        Args:
            task: Task to be marked as finished.
            result: result of the task execution, such as status and summery information.

        Returns:
            Task to be marked as started.
        """
        task.end_time = datetime.now()
        task.success = result.success
        task.summery = result.summery

        report = f"finished {task.task_name()} in {task.end_time - task.start_time} with status: {'success' if result.success else 'failed'}"
        if result.error is not None:
            report += f", error: \n{result.error}"
        else:
            # for the logger, remove entries with 0, but keep them in the original for reporting
            cleaned_summery = {key: value for key, value in result.summery.items() if value != 0}
            if len(cleaned_summery) > 0:
                report += f"\n{tabulate([cleaned_summery], headers='keys', tablefmt='psql')}"
        self.logger.info(report)
        return task

    def report_progress(self, task: Task, batches: int, expected_batches: int, stats: dict) -> None:
        """
        Optionally provide updates during execution of a task, such as batches processed so far.

        This is an optional call, as not all :py:class:`etl_lib.core.Task` need batching.

        Args:
            task: Task reporting updates.
            batches: Number of batches processed so far.
            expected_batches: Number of expected batches. Can be `None` if the overall number of
                batches is not known before execution.
            stats: dict of statistics so far (such as `nodes_created`).
        """
        pass

    def __print_tree(self, task: Task, last=True, header='') -> str:
        """Generates a tree view of the task tree."""
        elbow = "└──"
        pipe = "│  "
        tee = "├──"
        blank = "   "
        tree_string = header + (elbow if last else tee) + task.task_name() + "\n"
        if isinstance(task, TaskGroup):
            children = list(task.sub_tasks())
            for i, c in enumerate(children):
                tree_string += self.__print_tree(c, header=header + (blank if last else pipe),
                                                 last=i == len(children) - 1)
        return tree_string


class Neo4jProgressReporter(ProgressReporter):
    """
    Extends the ProgressReporter to additionally write the status updates from the tasks to a Neo4j database.
    """

    def __init__(self, context, database: str):
        """
        Creates a new Neo4j progress reporter.

        Args:
            context: :py:class:`etl_lib.core.ETLContext` containing a Neo4jConnection instance.
            database: Name of the database to write the status updates to.
        """
        super().__init__(context)
        self.run_uuid = None
        self.database = database
        self.logger.info(f"progress reporting to database: {self.database}")
        self.__create_constraints()
        self._register_shutdown_handler()

    def register_tasks(self, root: Task, **kwargs):
        super().register_tasks(root)

        self.run_uuid = root.uuid
        with self.context.neo4j.session(self.database) as session:
            order = 0
            session.run(
                "CREATE (t:ETLTask:ETLRun {uuid:$id, task:$task, order:$order, name:$name, status: 'open'}) SET t +=$other",
                id=root.uuid, order=order, task=root.__repr__(), name=root.task_name(), other=kwargs)
            self.__persist_task(session, root, order)

    def __persist_task(self, session, task: Task | TaskGroup, order: int) -> int:
        """Writes task information to the database."""

        if type(task) is Task:
            order += 1
            session.run(
                """
                MERGE (t:ETLTask { uuid: $id })
                    SET t.task=$task, t.order=$order, t.name=$name, t.status='open'
                """,
                id=task.uuid, task=task.__repr__(), order=order, name=task.task_name())
        else:
            for child in task.sub_tasks():
                order += 1
                session.run(
                    """
                    MATCH (p:ETLTask { uuid: $parent_id }) SET p.type='TaskGroup'
                    CREATE (t:ETLTask { uuid:$id, task:$task, order:$order, name:$name, status: 'open' })
                    CREATE (p)-[:HAS_SUB_TASK]->(t)
                    """,
                    parent_id=task.uuid, id=child.uuid, task=child.__repr__(), order=order, name=child.task_name())
                if isinstance(child, TaskGroup):
                    order = self.__persist_task(session, child, order)
        return order

    def started_task(self, task: Task) -> Task:
        super().started_task(task=task)
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask { uuid: $id }) SET t.startTime = $start_time, t.status= 'running'",
                        id=task.uuid,
                        start_time=task.start_time)
        return task

    def finished_task(self, task: Task, result: TaskReturn) -> Task:
        super().finished_task(task=task, result=result)
        if result.success:
            status = "success"
        else:
            status = "failure"
        with self.context.neo4j.session(self.database) as session:
            session.run("""
            MATCH (t:ETLTask {uuid:$id}) SET t.endTime = $end_time, t.status = $status, t.error = $error
            CREATE (s:ETLStats) SET s=$summery
            CREATE (t)-[:HAS_STATS]->(s)
            """, id=task.uuid, end_time=task.end_time, summery=result.summery, status=status, error=result.error)
        return task

    def __create_constraints(self):
        with self.context.neo4j.session(self.database) as session:
            session.run("CREATE CONSTRAINT etl_task_unique IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE;")

    def report_progress(self, task: Task, batches: int, expected_batches: int, stats: dict) -> None:
        self.logger.debug(f"{batches=}, {expected_batches=}, {stats=}")
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask {uuid:$id}) SET t.batches =$batches, t.expected_batches =$expected_batches",
                        id=task.uuid, batches=batches, expected_batches=expected_batches)

    def _register_shutdown_handler(self):
        def shutdown_handler(signum, frame):
            self.logger.warning("SIGINT received, waiting for running tasks to abort.")
            with self.context.neo4j.session(self.database) as session:
                cnt = session.run("""
                MATCH path=(r:ETLRun {uuid: $runId})-[*]->() 
                WITH [task in nodes(path) WHERE task:ETLTask AND task.status IN ['open', 'running'] | task] AS tasks
                UNWIND tasks AS task
                SET task.status = 'aborted'
                RETURN count(task) AS cnt
                """, runId=self.run_uuid
                ).single()['cnt']
            self.logger.info(f"marked {cnt} tasks as aborted.")
        add_sigint_handler(shutdown_handler)


def get_reporter(context) -> ProgressReporter:
    """
    Returns a ProgressReporter instance.

    If the :class:`ETLContext <etl_lib.core.ETLContext>` env holds the key `REPORTER_DATABASE` then
    a :class:`Neo4jProgressReporter` instance is created with the given database name.

    Otherwise, a  :class:`ProgressReporter` (no logging to database) instance will be created.
    """

    db = context.env("REPORTER_DATABASE")
    if db is None:
        return ProgressReporter(context)
    else:
        return Neo4jProgressReporter(context, db)
