import logging
import uuid
from datetime import datetime

from anytree import Node, RenderTree, ContStyle, Resolver, PreOrderIter
from tabulate import tabulate

from etl_lib.core.Task import Task, TaskGroup


class ProgressReporter:
    start_time: datetime
    end_time: datetime
    root_task: Node
    current_task: Node

    def __init__(self, context):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_tasks(self, main: Task):
        self.root_task = Node(name=main.task_name(), task=main, uuid=str(uuid.uuid4()))
        self.current_task = self.root_task

        if isinstance(main, TaskGroup):
            self.__add_subtree(self.root_task, main.sub_tasks())
        self.logger.info(
            f"the following tasks are registered for execution:\n{RenderTree(self.root_task, style=ContStyle())}")

    def started_task(self, task_name: str) -> Node:
        if self.current_task.name == task_name:
            # should only happen for root node?
            pass
        else:
            resolver = Resolver('name')
            self.current_task = resolver.get(self.current_task, task_name)
        self.current_task.start_time = datetime.now()
        self.logger.info(f"{'\t' * self.current_task.depth}starting {task_name}")
        return self.current_task

    def finished_task(self, success: bool, summery: dict) -> Node:
        task = self.current_task
        task.end_time = datetime.now()
        task.success = success
        task.summery = summery

        # for the logger, remove entries with 0, but keep them in the original for reporting
        cleaned_summery = {key: value for key, value in summery.items() if value != 0}
        self.logger.info(
            (f"{'\t' * task.depth}finished {task.name} with success: {success}"
             f" in {task.end_time - task.start_time}"
             f"\n{tabulate([cleaned_summery], headers='keys', tablefmt='psql')}")
        )
        self.current_task = self.current_task.parent
        return task

    def report_progress(self, task: Task, progress: int):
        pass

    def __add_subtree(self, parent: Node, children: list[Task | TaskGroup]):
        for child in children:
            node = Node(name=child.task_name(), parent=parent, task=child, uuid=str(uuid.uuid4()))
            if isinstance(child, TaskGroup):
                self.__add_subtree(node, child.sub_tasks())


class Neo4jProgressReporter(ProgressReporter):

    def __init__(self, context, database: str):
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
                    session.run(
                        "CREATE (t:ETLTask:ETLRun {uuid:$id, task:$task, order:$order, name:$name}) SET t+=$kwargs",
                        id=node.uuid, order=order, task=node.task.__repr__(), name=node.name, kwargs=kwargs)
                else:
                    session.run(
                        """
                        MATCH (p:ETLTask {uuid:$parent_id})
                        CREATE (t:ETLTask {uuid:$id, task:$task, order:$order, name:$name})
                        CREATE (p)-[:HAS_SUB_TASK]->(t)
                        """,
                        id=node.uuid, parent_id=node.parent.uuid, task=node.task.__repr__(), order=order,
                        name=node.name)
                order = order + 1

    def started_task(self, task_name: str) -> Node:
        node = super().started_task(task_name=task_name)
        with self.context.neo4j.session(self.database) as session:
            session.run("MATCH (t:ETLTask {uuid:$id}) SET t.startTime = $start_time", id=node.uuid,
                        start_time=node.start_time)
        return node

    def finished_task(self, success: bool, summery: dict) -> Node:
        node = super().finished_task(success=success, summery=summery)
        with self.context.neo4j.session(self.database) as session:
            session.run("""
            MATCH (t:ETLTask {uuid:$id}) SET t.endTime = $end_time, t.status = $status
            CREATE (s:ETLStats) SET s=$summery
            CREATE (t)-[:HAS_STATS]->(s)
            """, id=node.uuid, end_time=node.end_time, summery=summery, status=success)
        return node

    def __create_constraints(self):
        with self.context.neo4j.session(self.database) as session:
            session.run("CREATE CONSTRAINT etl_task_unique IF NOT EXISTS FOR (n:ETLTask) REQUIRE n.uuid IS UNIQUE;")


def get_reporter(context) -> ProgressReporter:
    db = context.env("REPORTER_DATABASE")
    if db is None:
        return ProgressReporter(context)
    else:
        return Neo4jProgressReporter(context, db)
