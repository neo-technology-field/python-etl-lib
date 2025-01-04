import abc
import logging
from collections import Counter
from collections import namedtuple
from datetime import datetime
from typing import override

from tabulate import tabulate

from etl_lib.ETLContext import ETLContext

InternalResult = namedtuple("InternalResult", ["success", "summery"])
"""
Returned from the Task.run_internal() function. Used to simplify the implementation of Tasks.
The `summery` field is the same as in `TaskReturn.summery`.
"""


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
    start_time: datetime
    end_time: datetime = None

    def __init__(self, success: bool = True, summery: dict = None, start_time: datetime = datetime.now(),
                 end_time: datetime = None):
        self.success = success
        self.summery = summery if summery else {}
        self.start_time = start_time
        self.end_time = end_time

    def mark_ended(self, result: InternalResult):
        self.success = result.success
        self.end_time = datetime.now()
        self.summery = result.summery if result.summery else {}

    def pretty_print(self):
        return (f"success: {self.success}, finished in {self.end_time - self.start_time}"
                f"\n{tabulate([self.summery], headers='keys', tablefmt='psql')}")


class Task:
    """
    Main building block. Everything that can be executed should derive from this class.
    Functionality is limited to some bookkeeping and logging, while allowing easy implementation.
    """
    context: ETLContext

    def __init__(self, context: ETLContext, log_indent: int = 1):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)
        self.log_indent = log_indent

    def execute(self, **kwargs) -> TaskReturn:
        """
        Executes the task. Implementations of this Interface should not overwrite this method, but provide the
        Task functionality inside `run_internal` which will be called from here.
        """
        task_return = TaskReturn()
        self.logger.info(f"{self.__indent()}starting {self.__class__.__name__}")

        # TODO handle exceptions on this level
        result = self.run_internal(**kwargs)
        task_return.mark_ended(result)

        self.logger.info(task_return.pretty_print())

        return task_return

    @abc.abstractmethod
    def run_internal(self, **kwargs) -> InternalResult:
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


class TaskGroup(Task):
    """
    Base class to allow wrapping of Task to form a hierarchy.
    Implementers should only need to provide the Tasks to execute as a block.
    The summery statistic object returned from the group execute method will be a merged/aggregated one.
    """

    def __init__(self, context: ETLContext, tasks: list[Task], log_indent: int = 1):
        super().__init__(context, log_indent)
        self.tasks = tasks

    @abc.abstractmethod
    def task_definitions(self) -> [Task]:
        """
        Implementers provide the Tasks to be executed. The order on the returned array will tbe the order of the
        execution of the tasks.
        If one Task returns success==False and abort_on_fail()==True, then the execution will stop at this point.

        :return:
        """
        pass

    def run_internal(self, **kwargs) -> InternalResult:
        summery = {}
        for task in self.tasks:
            ret = task.execute(**kwargs)
            summery = merge_summery(summery, ret.summery)
            if ret.success == False and task.abort_on_fail():
                return InternalResult(False, summery)
        return InternalResult(True, summery)

    def abort_on_fail(self):
        for task in self.tasks:
            if task.abort_on_fail():
                return True

