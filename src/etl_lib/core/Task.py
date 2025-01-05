import abc
import logging
from collections import Counter


def merge_summery(summery_1: dict, summery_2: dict) -> dict:
    """
    Helper function to merge dicts. Assuming that values are numbers.
    If a key exists in both dicts, then the result will contain a key with the added values.
    """
    return dict(Counter(summery_1) + Counter(summery_2))


class TaskReturn:
    """
    Return object for the Task.execute() function, containing results and timing information.
    The contained `summery` dict can be used t by tasks to return statistics about the job performed,
    such as rows inserted, updated, ...
    """
    success: bool
    summery: dict

    def __init__(self, success: bool = True, summery: dict = None):
        self.success = success
        self.summery = summery if summery else {}


class Task:
    """
    Main building block. Everything that can be executed should derive from this class.
    Functionality is limited to some bookkeeping and logging, while allowing easy implementation.
    """

    def __init__(self, context, log_indent: int = 1):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)
        self.log_indent = log_indent

    def execute(self, **kwargs) -> TaskReturn:
        """
        Executes the task. Implementations of this Interface should not overwrite this method, but provide the
        Task functionality inside `run_internal` which will be called from here.
        """
        task_return = TaskReturn()
        self.context.reporter.started_task(self.task_name())

        # TODO handle exceptions on this level
        result = self.run_internal(**kwargs)

        self.context.reporter.finished_task(result.success, result.summery)

        return task_return

    @abc.abstractmethod
    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Abstract method that implementations must implement for the actual job of the task.
        :return: Tuple containing if the success or failure of the Task as well as statistics.
        """
        pass

    def __indent(self):
        return '\t' * self.log_indent

    def abort_on_fail(self) -> bool:
        """
        Returning `True` here indicates to the caller that no other Tasks should be executed if this task return
        success==False from `run_internal`
        :return:
        """
        return True

    def task_name(self) -> str:
        return self.__class__.__name__

    def __repr__(self):
        return f"Task({self.task_name()})"


class TaskGroup(Task):
    """
    Base class to allow wrapping of Task to form a hierarchy.
    Implementers should only need to provide the Tasks to execute as a block.
    The summery statistic object returned from the group execute method will be a merged/aggregated one.
    """

    def __init__(self, context, tasks: list[Task], name: str, log_indent: int = 1):
        super().__init__(context, log_indent)
        self.tasks = tasks
        self.name = name

    def sub_tasks(self) -> [Task]:
        return self.tasks

    def run_internal(self, **kwargs) -> TaskReturn:
        summery = {}
        for task in self.tasks:
            ret = task.execute(**kwargs)
            summery = merge_summery(summery, ret.summery)
            if ret.success == False and task.abort_on_fail():
                return TaskReturn(False, summery)
        return TaskReturn(True, summery)

    def abort_on_fail(self):
        for task in self.tasks:
            if task.abort_on_fail():
                return True

    def task_name(self) -> str:
        return self.name

    def __repr__(self):
        return f"TaskGroup({self.task_name()})"
