import abc
import logging
import sys
from dataclasses import dataclass, field
from typing import Generator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.Task import Task
from etl_lib.core.utils import merge_summery


@dataclass
class BatchResults:
    """
    Return object of the `get_batch()` method, wrapping a batch together with meta information.
    """
    chunk: []
    """The batch of data."""
    statistics: dict = field(default_factory=dict)
    """`dict` of statistic information, such as row processed, nodes writen, .."""
    batch_size: int = field(default=sys.maxsize)
    """size of the batch."""


def append_result(org: BatchResults, stats: dict) -> BatchResults:
    return BatchResults(chunk=org.chunk, statistics=merge_summery(org.statistics, stats),
                        batch_size=org.batch_size)


class BatchProcessor:
    """
    Allows assembly of Tasks out of smaller building blocks.
    This way, functionally such as reading from a CSV file, writing to a database or validation
    can be implemented independently and re-used.
    BatchProcessors process data in batches. A batch of data is requested from the provided predecessor
    and returned in batches to the caller. Usage of `Generators` ensure that not all data must be loaded at once.
    """
    def __init__(self, context: ETLContext, task:Task, predecessor=None):
        self.context = context
        self.predecessor = predecessor
        self.logger = logging.getLogger(self.__class__.__name__)
        self.task = task

    @abc.abstractmethod
    def get_batch(self, max_batch__size: int) -> Generator[BatchResults, None, None]:
        """
        Returns a generator that yields processed batches from the provided predecessor.
        :param max_batch__size: The max size of the batch the caller expects to receive.
        :return: A generator that yields processed batches.
        """
        pass
