import logging

import pytest
from neo4j import Driver

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.ProgressReporter import Neo4jProgressReporter
from etl_lib.core.Task import Task, TaskReturn, TaskGroup
from etl_lib.test_utils.utils import TestNeo4jContext, get_database_name


class TestETLContext(ETLContext):

    def __init__(self, driver: Driver):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__env_vars = {}
        self.neo4j = TestNeo4jContext(driver)
        self.reporter = Neo4jProgressReporter(self, get_database_name())


@pytest.fixture
def context(neo4j_driver_with_empty_db) -> ETLContext:
    return TestETLContext(neo4j_driver_with_empty_db)


class Task1(Task):
    def run_internal(self, **kwargs) -> TaskReturn:
        pass


class Task2(Task):
    def run_internal(self, **kwargs) -> TaskReturn:
        pass


class Task3(Task):
    def run_internal(self, **kwargs) -> TaskReturn:
        pass


def test_etl_db_structure(context, neo4j_driver):
    task1 = Task1(context)
    task2 = Task2(context)
    group_1 = TaskGroup(context, [task1, task2], "group 1")

    task3 = Task3(context)
    root = TaskGroup(context, [group_1, task3], "root")

    context.reporter.register_tasks(root, addon1="addon1", addon2="addon2")

    with neo4j_driver.session(database=get_database_name()) as session:
        result = session.run("""
        MATCH (r:ETLRun:ETLTask {uuid:$run_id})-[:HAS_SUB_TASK*]->(task)
        WITH r, task ORDER BY task.order ASC
        RETURN r{ .name, .status, .task, .uuid, .order, .addon1, .addon2 } AS run, 
            COLLECT( task{ .name, .status, .task, .uuid, .order }) AS tasks
        """, run_id=root.uuid).single()
        tasks = result["tasks"]
        run = result["run"]

    expected = [{'name': 'group 1', 'order': 1, 'status': 'open', 'task': 'TaskGroup(group 1)',
                 'uuid': group_1.uuid},
                {'name': 'Task1', 'order': 2, 'status': 'open', 'task': 'Task(Task1)',
                 'uuid': task1.uuid},
                {'name': 'Task2', 'order': 3, 'status': 'open', 'task': 'Task(Task2)',
                 'uuid': task2.uuid},
                {'name': 'Task3', 'order': 4, 'status': 'open', 'task': 'Task(Task3)',
                 'uuid': task3.uuid}]
    assert tasks == expected
    assert run == {'addon1': 'addon1', 'addon2': 'addon2', 'name': 'root', 'order': 0, 'status': 'open',
                   'task': 'TaskGroup(root)', 'uuid': root.uuid}
