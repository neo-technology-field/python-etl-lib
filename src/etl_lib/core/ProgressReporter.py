import logging
from datetime import datetime

from tabulate import tabulate

from etl_lib.core.Task import Task, TaskGroup


class ProgressReporter:
    """
    ProgressReporter that reports status updates from the tasks through the python `logging` package.
    """
    start_time: datetime
    end_time: datetime

    def __init__(self, context):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_tasks(self, main: Task):
        """
        Registers Tasks with this reporter, esp. needed for derived reporters that need to prepare tasks for later reporting
        Tasks need to contain an `uuid` member to uniquely identify a task in the tree.
        :param main: Root of the task tree.
        """
        self.logger.info("\n" + self.print_tree(main))

    def started_task(self, task: Task) -> Task:
        """
        Marks the task as started. Start the time keeping for this task.
        :param task: Task to be marked as started.
        :return: The current task that was started.
        """
        task.start_time = datetime.now()
        self.logger.info(f"{'\t' * task.depth}starting {task.task_name()}")
        return task

    def finished_task(self, task: Task, success: bool, summery: dict, error: str = None) -> Task:
        """
        Marks the task as finished. Finish the time keeping for this task.
        This does not change the current task.
        :param task: Task to be marked as finished.
        :param success: True if the task was successfully finished.
        :param summery: dict of statistics for this task (such as `nodes_created`)
        :param error: If an exception occurred, the exception test should be provided here.
        :return: The current task that was finished.
        """
        task.end_time = datetime.now()
        task.success = success
        task.summery = summery

        report = f"{'\t' * task.depth}finished {task.task_name()} with success: {success}"
        if error is not None:
            report += f", error: \n{error}"
        else:
            # for the logger, remove entries with 0, but keep them in the original for reporting
            cleaned_summery = {key: value for key, value in summery.items() if value != 0}
            if len(cleaned_summery) > 0:
                report += f"\n{tabulate([cleaned_summery], headers='keys', tablefmt='psql')}"
        self.logger.info(report)
        return task

    def report_progress(self, task: Task, batches: int, expected_batches: int, stats: dict) -> None:
        """
        Optionally provide updates during execution of a task, most likely at the end of each batch.
        :param task: Task reporting updates.
        :param batches: Number of batches processed so far.
        :param expected_batches: Number of expected batches. Can be `None` if the overall number of
                batches is not know before execution.
        :param stats: dict of statistics so far (such as `nodes_created`)
        """
        pass

    def print_tree(self, task: Task, last=True, header='') -> str:
        elbow = "└──"
        pipe = "│  "
        tee = "├──"
        blank = "   "
        tree_string = header + (elbow if last else tee) + task.task_name() + "\n"
        if isinstance(task, TaskGroup):
            children = list(task.sub_tasks())
            for i, c in enumerate(children):
                tree_string += self.print_tree(c, header=header + (blank if last else pipe),
                                               last=i == len(children) - 1)
        return tree_string


class Neo4jProgressReporter(ProgressReporter):
    """
    Extends the ProgressReporter to additionally write the status updates from the tasks to a Neo4j database.
    """

    def __init__(self, context, database: str):
        """
        Creates a new Neo4j progress reporter.
        :param context: ETLContext containing a Neo4jConnection instance.
        :param database: Name of the database to write the status updates to.
        """
        super().__init__(context)
        self.database = database
        self.logger.info(f"progress reporting to database: {self.database}")
        self.__create_constraints()

    def register_tasks(self, root: Task, **kwargs):
        super().register_tasks(root)

        with self.context.neo4j.session(self.database) as session:
            order = 0
            session.run(
                "CREATE (t:ETLTask:ETLRun {uuid:$id, task:$task, order:$order, name:$name, status: 'open'}) SET t +=$other",
                id=root.uuid, order=order, task=root.__repr__(), name=root.task_name(), other=kwargs)
            self.persist_task(session, root, order)

    def persist_task(self, session, task: Task | TaskGroup, order: int) -> int:

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
                    order = self.persist_task(session, child, order)
        return order

    def started_task(self, task: Task) -> Task:
        super().started_task(task=task)
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask { uuid: $id }) SET t.startTime = $start_time, t.status= 'running'",
                        id=task.uuid,
                        start_time=task.start_time)
        return task

    def finished_task(self, task: Task, success: bool, summery: dict, error: str = None) -> Task:
        super().finished_task(task=task, success=success, summery=summery, error=error)
        if success:
            status = "success"
        else:
            status = "failure"
        with self.context.neo4j.session(self.database) as session:
            session.run("""
            MATCH (t:ETLTask {uuid:$id}) SET t.endTime = $end_time, t.status = $status, t.error = $error
            CREATE (s:ETLStats) SET s=$summery
            CREATE (t)-[:HAS_STATS]->(s)
            """, id=task.uuid, end_time=task.end_time, summery=summery, status=status, error=error)
        return task

    def __create_constraints(self):
        with self.context.neo4j.session(self.database) as session:
            session.run("CREATE CONSTRAINT etl_task_unique IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE;")

    def report_progress(self, task: Task, batches: int, expected_batches: int, stats: dict) -> None:
        self.logger.debug(f"{batches=}, {expected_batches=}, {stats=}")
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask {uuid:$id}) SET t.batches =$batches, t.expected_batches =$expected_batches",
                        id=task.uuid, batches=batches, expected_batches=expected_batches)


def get_reporter(context) -> ProgressReporter:
    db = context.env("REPORTER_DATABASE")
    if db is None:
        return ProgressReporter(context)
    else:
        return Neo4jProgressReporter(context, db)
