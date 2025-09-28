import abc
import logging
import sys
from dataclasses import dataclass, field
from typing import Generator, List, Any

from etl_lib.core.Task import Task
from etl_lib.core.utils import merge_summery


@dataclass
class BatchResults:
    """
    Return object of the :py:func:`~BatchProcessor.get_batch` method, wrapping a batched data together with meta information.
    """
    chunk: List[Any]
    """The batch of data."""
    statistics: dict = field(default_factory=dict)
    """`dict` of statistic information, such as row processed, nodes writen, .."""
    batch_size: int = field(default=sys.maxsize)
    """size of the batch."""


def append_result(org: BatchResults, stats: dict) -> BatchResults:
    """
    Appends the stats dict to the provided `org`.

    Args:
        org: The original `BatchResults` object.
        stats: dict containing statistics to be added to the org object.

    Returns:
        New `BatchResults` object, where the :py:attr:`~BatchResults.statistics` attribute is the merged result of the
            provided parameters. Values in the dicts with the same key are added.

    """
    return BatchResults(chunk=org.chunk, statistics=merge_summery(org.statistics, stats),
                        batch_size=org.batch_size)


class BatchProcessor(abc.ABC):
    """
    Allows assembly of :py:class:`etl_lib.core.Task.Task` out of smaller building blocks.

    This way, functionally, such as reading from a CSV file, writing to a database or validation
    can be implemented and tested independently and re-used.

    BatchProcessors form, a linked list, where each processor only knows about its predecessor.

    BatchProcessors process data in batches. A batch of data is requested from the provided predecessors
    :py:func:`~get_batch`
    and returned in batches to the caller. Usage of `Generators` ensure that not all data must be loaded at once.
    """

    def __init__(self, context, task: Task = None, predecessor=None):
        """
        Constructs a new :py:class:`etl_lib.core.BatchProcessor` instance.

        Args:
            context: :py:class:`etl_lib.core.ETLContext.ETLContext` instance. It Will be available to subclasses.
            task: :py:class:`etl_lib.core.Task.Task` this processor is part of.
                Needed for status reporting only.
            predecessor: Source of batches for this processor.
                Can be `None` if no predecessor is needed (such as when this processor is the start of the queue).
        """
        self.context = context
        """:py:class:`etl_lib.core.ETLContext.ETLContext` instance. Providing access to general facilities."""
        self.predecessor = predecessor
        """Predecessor, used as a source of batches."""
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self.task = task
        """The :py:class:`etl_lib.core.Task.Task` owning instance."""

    @abc.abstractmethod
    def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
        """
        Provides a batch of data to the caller.

        The batch itself could be called and processed from the provided predecessor or generated from other sources.

        Args:
            max_batch__size: The max size of the batch the caller expects to receive.

        Returns
            A generator that yields batches.
        """
        pass
