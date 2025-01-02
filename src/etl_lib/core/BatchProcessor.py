import abc
import logging
import sys
from dataclasses import dataclass, field
from typing import Generator

from etl_lib.core.Task import merge_summery


@dataclass
class BatchResults:
    chunk: []
    statistics: dict = field(default_factory=dict)
    batch_size: int = field(default=sys.maxsize)


def append_result(org: BatchResults, stats: dict) -> BatchResults:
    return BatchResults(chunk=org.chunk, statistics=merge_summery(org.statistics, stats),
                        batch_size=org.batch_size)


class BatchProcessor:

    def __init__(self, predecessor):
        self.predecessor = predecessor
        self.logger = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    def get_batch(self, batch_size: int) -> Generator[BatchResults, None, None]:
        pass
