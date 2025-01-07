from pathlib import Path
from typing import Any

from anytree import Node

from etl_lib.ETLContext import ETLContext, Neo4jContext, QueryResult
from etl_lib.core.ProgressReporter import ProgressReporter
from etl_lib.core.Task import Task


def get_test_file(file_name):
    """
    Returns the absolut path of a file that exists in `tests/data/` directory.
    """
    return Path(__file__).parent / f"../data/{file_name}"

class DummyReporter:

    def register_tasks(self, main: Task):
       pass

    def started_task(self, task: Task) -> Node:
        pass

    def finished_task(self, success: bool, summery: dict, error: str = None) -> Node:
        pass

    def report_progress(self, batches: int, expected_batches: int, stats: dict) -> None:
        pass


class DummyNeo4jContext:

    def query_database(self, session, query, **kwargs) -> QueryResult:
        return QueryResult([], {})

    def session(self, database=None):
        return None


class DummyContext:
    neo4j: DummyNeo4jContext
    __env_vars: dict
    path_error: Path
    path_import: Path
    path_processed: Path
    reporter = DummyReporter()

    def env(self, key: str) -> Any:
        pass
