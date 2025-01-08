import logging
from datetime import datetime

from anytree import Node, RenderTree, ContStyle, Resolver, PreOrderIter
from tabulate import tabulate

from etl_lib.core.Task import Task, TaskGroup


class ProgressReporter:
    """
    ProgressReporter that reports status updates from the tasks through the python `logging` package.
    """
    start_time: datetime
    end_time: datetime
    root_task: Node
    current_task: Node

    def __init__(self, context):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_tasks(self, main: Task):
        """
        Registers Tasks with this reporter, such that later `started_task()` and `finished_task()` calls are handled
        correctly.
        Tasks need to contain an `uuid` member to uniquely identify a task in the tree.
        :param main: Root of the task tree.
        """
        self.root_task = Node(name=main.task_name(), task=main, uuid=main.uuid)
        self.current_task = self.root_task

        if isinstance(main, TaskGroup):
            self.__add_subtree(self.root_task, main.sub_tasks())
        self.logger.info(
            f"the following tasks are registered for execution:\n{RenderTree(self.root_task, style=ContStyle())}")

    def started_task(self, task: Task) -> Node:
        """
        Marks the task as started. Start the time keeping for this task.
        Task must be a child of the last started task or the root task.
        :param task: Task tobe marked as started.
        :return: The current task that was started.
        """
        if self.current_task.name != task.task_name():
            resolver = Resolver('uuid')
            self.current_task = resolver.get(self.current_task, task.uuid)

        self.current_task.start_time = datetime.now()
        self.logger.info(f"{'\t' * self.current_task.depth}starting {task.task_name()}")
        return self.current_task

    def finished_task(self, success: bool, summery: dict, error: str = None) -> Node:
        """
        Marks the task as finished. Finish the time keeping for this task.
        This does not change the current task.
        :param success: True if the task was successfully finished.
        :param summery: dict of statistics for this task (such as `nodes_created`)
        :param error: If an exception occurred, the exception test should be provided here.
        :return: The current task that was finished.
        """
        task = self.current_task
        task.end_time = datetime.now()
        task.success = success
        task.summery = summery

        report = f"{'\t' * task.depth}finished {task.name} with success: {success}"
        if error is not None:
            report += f", error: \n{error}"
        else:
            # for the logger, remove entries with 0, but keep them in the original for reporting
            cleaned_summery = {key: value for key, value in summery.items() if value != 0}
            if len(cleaned_summery) > 0:
                report += f"\n{tabulate([cleaned_summery], headers='keys', tablefmt='psql')}"
        self.logger.info(report)
        self.current_task = self.current_task.parent
        return task

    def report_progress(self, batches: int, expected_batches: int, stats: dict) -> None:
        """
        Optionally provide updates during execution of a task, most likely at the end of each batch.
        :param batches: Number of batches processed so far.
        :param expected_batches: Number of expected batches. Can be `None` if the overall number of
                batches is not know before execution.
        :param stats: dict of statistics so far (such as `nodes_created`)
        """
        pass

    def __add_subtree(self, parent: Node, children: list[Task | TaskGroup]):
        for child in children:
            node = Node(name=child.task_name(), parent=parent, task=child, uuid=child.uuid)
            if isinstance(child, TaskGroup):
                self.__add_subtree(node, child.sub_tasks())


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

    def register_tasks(self, main: Task, **kwargs):
        super().register_tasks(main)

        with self.context.neo4j.session(self.database) as session:
            order = 1
            for node in PreOrderIter(self.root_task):
                if node.is_root:
                    addons = kwargs | main.addons
                    session.run(
                        "CREATE (t:ETLTask:ETLRun {uuid:$id, task:$task, order:$order, name:$name, "
                        "type:'TaskGroup', status: 'open'}) "
                        "SET t+=$addons",
                        id=node.uuid, order=order, task=node.task.__repr__(), name=node.name, addons=addons)
                else:
                    type_ = "Task"
                    if isinstance(node.task, TaskGroup):
                        type_ = "TaskGroup"
                    session.run(
                        """
                        MATCH (p:ETLTask {uuid:$parent_id})
                        CREATE (t:ETLTask {uuid:$id, task:$task, order:$order, name:$name, type:$type, status: 'open'})
                        SET t+=$addons
                        CREATE (p)-[:HAS_SUB_TASK]->(t)
                        """,
                        id=node.uuid, parent_id=node.parent.uuid, task=node.task.__repr__(), order=order,
                        name=node.name, type=type_, addons=node.task.addons)
                order = order + 1

    def started_task(self, task: Task) -> Node:
        node = super().started_task(task=task)
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask {uuid:$id}) SET t.startTime = $start_time, t.status= 'running'",
                        id=node.uuid,
                        start_time=node.start_time)
        return node

    def finished_task(self, success: bool, summery: dict, error: str = None) -> Node:
        node = super().finished_task(success=success, summery=summery, error=error)
        if success:
            status = "success"
        else:
            status = "failure"
        with self.context.neo4j.session(self.database) as session:
            session.run("""
            MATCH (t:ETLTask {uuid:$id}) SET t.endTime = $end_time, t.status = $status, t.error = $error
            CREATE (s:ETLStats) SET s=$summery
            CREATE (t)-[:HAS_STATS]->(s)
            """, id=node.uuid, end_time=node.end_time, summery=summery, status=status, error=error)
        return node

    def __create_constraints(self):
        with self.context.neo4j.session(self.database) as session:
            session.run("CREATE CONSTRAINT etl_task_unique IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE;")

    def report_progress(self, batches: int, expected_batches: int, stats: dict) -> None:
        self.logger.debug(f"{batches=}, {expected_batches=}, {stats=}")
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask {uuid:$id}) SET t.batches =$batches, t.expected_batches =$expected_batches",
                        id=self.current_task.uuid, batches=batches, expected_batches=expected_batches)


def get_reporter(context) -> ProgressReporter:
    db = context.env("REPORTER_DATABASE")
    if db is None:
        return ProgressReporter(context)
    else:
        return Neo4jProgressReporter(context, db)
