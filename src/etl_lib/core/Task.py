import abc
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


class TaskReturn:
    """
    Return object for the :func:`~Task.execute` function, transporting result information.
    """

    success: bool
    """Success or failure of the task."""
    summery: dict
    """dict holding statistics about the task performed, such as rows inserted, updated."""
    error: str
    """Error message."""

    def __init__(self, success: bool = True, summery: dict = None, error: str = None):
        self.success = success
        self.summery = summery if summery else {}
        self.error = error

    def __repr__(self):
        return f"TaskReturn({self.success=}, {self.summery=}, {self.error=})"

    def __add__(self, other):
        """
        Adding 2 instances of TaskReturn.

        Args:
            other: Instance to add.

        Returns:
              New TaskReturn instance. `success` is the logical AND of the instances.
              `summery` is the merged dict. For the values of the same key the values are added.
        """
        if not isinstance(other, TaskReturn):
            return NotImplemented

        # Merge the summery dictionaries by summing their values
        merged_summery = self.summery.copy()
        for key, value in other.summery.items():
            merged_summery[key] = merged_summery.get(key, 0) + value

        # Combine success values and errors
        combined_success = self.success and other.success
        combined_error = None if not (self.error or other.error) \
            else f"{self.error or ''} | {other.error or ''}".strip(" |")

        return TaskReturn(
            success=combined_success, summery=merged_summery, error=combined_error
        )


class Task:
    """
    ETL job that can be executed.

    Provides reporting, time tracking and error handling.
    Implementations must provide the :func:`~run_internal` function.
    """

    def __init__(self, context):
        """
        Construct a Task object.

        Args:
            context: :class:`~etl_lib.core.ETLContext.ETLContext` instance. Will be available to subclasses.
        """
        self.context = context
        """:class:`~etl_lib.core.ETLContext.ETLContext` giving access to various resources."""
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self.uuid = str(uuid.uuid4())
        """Uniquely identifies a Task."""
        self.start_time: datetime
        """Time when the :func:`~execute` was called., `None` before."""
        self.end_time: datetime
        """Time when the :func:`~execute` has finished., `None` before."""
        self.success: bool
        """True if the task has finished successful. False otherwise, `None` before the task has finished."""
        self.depth: int = 0
        """Level or depth of the task in the hierarchy. The root task is depth 0. Updated by the Reporter"""

    def execute(self, **kwargs) -> TaskReturn:
        """
        Executes the task.

        Implementations of this Interface should not overwrite this method, but provide the
        Task functionality inside :func:`~run_internal` which will be called from here.
        Will use the :class:`~etl_lib.core.ProgressReporter.ProgressReporter` from
        :attr:`~etl_lib.core.Task.Task.context` to report status updates.

        Args:
            kwargs: will be passed to `run_internal`
        """
        self.context.reporter.started_task(self)

        try:
            result = self.run_internal(**kwargs)
        except Exception as e:
            result = TaskReturn(success=False, summery={}, error=str(e))

        self.context.reporter.finished_task(task=self,result=result)

        return result

    @abc.abstractmethod
    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Place to provide the logic to be performed.

        This base class provides all the housekeeping and reporting, so that implementation must/should not need to care
        about them.
        Exceptions should not be captured by implementations. They are handled by this base class.

        Args:
            kwargs: will be passed to `run_internal`
        Returns:
            An instance of :py:class:`~etl_lib.core.Task.TaskReturn`.
        """
        pass

    def abort_on_fail(self) -> bool:
        """
        Should the pipeline abort when this job fails.

        Returns:
            `True` indicates that no other Tasks should be executed if :py:func:`~run_internal` fails.
        """
        return True

    def task_name(self) -> str:
        """
        Option to overwrite the name of this Task.

        Name is used in reporting only.

        Returns:
            Sting describing the task. Defaults to the class name..
        """
        return self.__class__.__name__

    def __repr__(self):
        return f"Task({self.task_name()})"


class TaskGroup(Task):
    """
    Base class to allow wrapping of Task or TaskGroups to form a hierarchy of jobs.

    Implementations only need to provide the Tasks to execute as an array.
    The summery statistic object returned from the group execute method will be a merged/aggregated one.
    """

    def __init__(self, context, tasks: list[Task], name: str):
        """
        Construct a TaskGroup object.

        Args:
            context: :py:class:`etl_lib.core.ETLContext.ETLContext` instance.
            tasks: a list of `:py:class:`etl_lib.core.Task.Rask` instances.
                These will be executed in the order provided when :py:func:`~run_internal` is called.
            name: short name of the TaskGroup for reporting.
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
                self.logger.warning(
                    f"Task {self.task_name()} failed. Aborting execution."
                )
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
    """
    Task group for parallel execution of jobs.

    This class uses a ThreadPoolExecutor to run the provided tasks :py:func:`~run_internal` functions in parallel.
    Care should be taken that the Tasks can operate without blocking.locking each other.
    """

    def __init__(self, context, tasks: list[Task], name: str):
        """
        Construct a TaskGroup object.

        Args:
            context: :py:class:`etl_lib.core.ETLContext.ETLContext` instance.
            tasks: an array of `Task` instances.
                These will be executed in parallel when :py:func:`~run_internal`  is called.
                The Tasks in the array could itself be other TaskGroups.
            name: short name of the TaskGroup.
        """
        super().__init__(context, tasks, name)

    def run_internal(self, **kwargs) -> TaskReturn:
        combined_result = TaskReturn()

        with ThreadPoolExecutor() as executor:
            future_to_task = {
                executor.submit(task.execute, **kwargs): task for task in self.tasks
            }

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
                    self.logger.error(
                        f"Task {task.task_name()} encountered an error: {str(e)}"
                    )
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
