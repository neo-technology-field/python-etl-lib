import abc
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


class TaskReturn:
    """
    Return object for the Task.execute() function, containing results and timing information.
    The contained `summery` dict can be used by tasks to return statistics about the job performed,
    such as rows inserted, updated, ...
    """
    success: bool
    summery: dict
    error: str

    def __init__(self, success: bool = True, summery: dict = None, error: str = None):
        self.success = success
        self.summery = summery if summery else {}
        self.error = error

    def __repr__(self):
        return f"TaskReturn({self.success=}, {self.summery=}, {self.error=})"

    def __add__(self, other):
        if not isinstance(other, TaskReturn):
            return NotImplemented

        # Merge the summery dictionaries by summing their values
        merged_summery = self.summery.copy()
        for key, value in other.summery.items():
            merged_summery[key] = merged_summery.get(key, 0) + value

        # Combine success values and errors
        combined_success = self.success and other.success
        combined_error = f"{self.error or ''} | {other.error or ''}".strip(" |")

        return TaskReturn(success=combined_success, summery=merged_summery, error=combined_error)


class Task:
    """
    Main building block. Everything that can be executed should derive from this class.
    Functionality is limited to some bookkeeping and logging, while allowing easy implementation.
    """

    def __init__(self, context):
        """
        Construct a Task object.
        :param context: `ETLContext` instance.
        """
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uuid = str(uuid.uuid4())
        """Uniquely identified a Task. Needed for for the Reporter mostly."""
        self.addons = {}
        """add anny additional attributes. these will be picked up by the ProgressReporter and stored in the graph. """
        self.start_time: datetime
        self.end_time: datetime
        self.success: bool
        self.summery: dict
        self.error: str
        self.depth: int = 0

    def execute(self, **kwargs) -> TaskReturn:
        """
        Executes the task. Implementations of this Interface should not overwrite this method, but provide the
        Task functionality inside `run_internal` which will be called from here.
        Will use the `Reporter` from the context to repost status updates.
        :param kwargs: will be passed to `run_internal`
        """
        self.context.reporter.started_task(self)

        try:
            result = self.run_internal(**kwargs)
        except Exception as e:
            result = TaskReturn(success=False, summery={}, error=str(e))

        self.context.reporter.finished_task(task=self, success=result.success, summery=result.summery,
                                            error=result.error)

        return result

    @abc.abstractmethod
    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Abstract method that implementations must implement for the actual job of the task.
        Exceptions should not be captured iby implementations. They will be handled by this class.
        :return: Tuple containing if the success or failure of the Task as well as statistics.
        """
        pass

    def abort_on_fail(self) -> bool:
        """
        Returning `True` here indicates to the caller that no other Tasks should be executed if this task return
        success==False from `run_internal`
        :return:
        """
        return True

    def task_name(self) -> str:
        """
        Option to overwrite the name of this Task. Name is used in reporting.
        :return: str describing the task.
        """
        return self.__class__.__name__

    def __repr__(self):
        return f"Task({self.task_name()})"


class TaskGroup(Task):
    """
    Base class to allow wrapping of Task or taskGroups to form a hierarchy.
    Implementers should only need to provide the Tasks to execute as an array.
    The summery statistic object returned from the group execute method will be a merged/aggregated one.
    """

    def __init__(self, context, tasks: list[Task], name: str):
        """
        Construct a TaskGroup object.
        :param context: `ETLContext` instance.
        :param tasks: a list of `Task` instances. These will be executed in the order provided when `run_internal`
                    is called.
        :param name: short name of the TaskGroup.
        """
        super().__init__(context)
        self.tasks = tasks
        self.name = name

    def sub_tasks(self) -> [Task]:
        return self.tasks

    def run_internal(self, **kwargs) -> TaskReturn:
        ret = TaskReturn()
        for task in self.tasks:
            task_ret = task.execute(**kwargs)
            if task_ret == False and task.abort_on_fail():
                self.logger.warning(f"Task {self.task_name()} failed. Aborting execution.")
                return task_ret
            ret = ret + task_ret
        return ret

    def abort_on_fail(self):
        for task in self.tasks:
            if task.abort_on_fail():
                return True

    def task_name(self) -> str:
        return self.name

    def __repr__(self):
        return f"TaskGroup({self.task_name()})"


class ParallelTaskGroup(TaskGroup):

    def __init__(self, context, tasks: list[Task], name: str):
        """
        Construct a TaskGroup object.
        :param context: `ETLContext` instance.
        :param tasks: a list of `Task` instances. These will be executed in parallel when `run_internal`
                    is called.
        :param name: short name of the TaskGroup.
        """
        super().__init__(context, tasks, name)

    def run_internal(self, **kwargs) -> TaskReturn:
        combined_result = TaskReturn()

        with ThreadPoolExecutor() as executor:
            future_to_task = {executor.submit(task.execute, **kwargs): task for task in self.tasks}

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    combined_result += result

                    # If a task fails and it has abort_on_fail set, stop further execution
                    if not result.success and task.abort_on_fail():
                        self.logger.warning(
                            f"Task {task.task_name()} failed. Aborting execution of TaskGroup {self.task_name()}."
                        )
                        # Cancel any pending tasks
                        for f in future_to_task:
                            if not f.done():
                                f.cancel()
                        return combined_result

                except Exception as e:
                    self.logger.error(f"Task {task.task_name()} encountered an error: {str(e)}")
                    error_result = TaskReturn(success=False, summery={}, error=str(e))
                    combined_result += error_result

                    # Handle abort logic for unexpected exceptions
                    if task.abort_on_fail():
                        self.logger.warning(
                            f"Unexpected failure in {task.task_name()}. Aborting execution of TaskGroup {self.task_name()}."
                        )
                        # Cancel any pending tasks
                        for f in future_to_task:
                            if not f.done():
                                f.cancel()
                        return combined_result

        return combined_result
