import logging
from datetime import datetime

from anytree import Node, RenderTree, ContStyle, Resolver
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
        self.root_task = Node(name=main.task_name(), task=main)
        self.current_task = self.root_task

        if isinstance(main, TaskGroup):
            self.__add_subtree(self.root_task, main.sub_tasks())
        self.logger.info(f"the following tasks are registered for execution:\n{RenderTree(self.root_task, style=ContStyle())}")

    def started_task(self, task_name: str):
        if self.current_task.name == task_name:
            # should only happen for root node?
            pass
        else:
            resolver = Resolver('name')
            self.current_task = resolver.get(self.current_task, task_name)
        self.current_task.start_time = datetime.now()
        self.logger.info(f"{'\t' * self.current_task.depth}starting {task_name}")

    def finished_task(self, success: bool, summery: dict):
        self.current_task.end_time = datetime.now()
        self.current_task.success = success
        self.current_task.summery = summery

        self.logger.info(
            (f"{'\t' * self.current_task.depth}finished {self.current_task.name} with success: {success}"
            f" in {self.current_task.end_time - self.current_task.start_time}"
            f"\n{tabulate([summery], headers='keys', tablefmt='psql')}")
            )
        #self.logger.info(task_return.pretty_print())
        self.current_task = self.current_task.parent

    def report_progress(self, task: Task, progress: int):
        pass

    def __add_subtree(self, parent: Node, children: list[Task | TaskGroup]):
        for child in children:
            node = Node(name=child.task_name(), parent=parent, task=child)
            if isinstance(child, TaskGroup):
                self.__add_subtree(node, child.sub_tasks())


class Neo4jProgressReporter(ProgressReporter):

    def __init__(self, context, database: str):
        super().__init__(context)
        self.database = database


def get_reporter(context) -> ProgressReporter:
    db = context.env("REPORTER_DATABASE")
    if db is None:
        return ProgressReporter(context)
    else:
        return Neo4jProgressReporter(context, db)
