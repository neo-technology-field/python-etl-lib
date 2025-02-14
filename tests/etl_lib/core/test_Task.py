from datetime import datetime

from etl_lib.core.Task import Task, TaskReturn, TaskGroup, ParallelTaskGroup
from etl_lib.core.utils import merge_summery
from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask
from etl_lib.test_utils.utils import DummyContext, DummyReporter


def test_merge_summery():
    merged = merge_summery({"a": 2, "b": 3}, {"b": 2, "c": 4})

    assert merged == {"a": 2, "b": 5, "c": 4}


def test_simple_task():
    class DummyTask(Task):
        def run_internal(self, *args, **kwargs):
            return TaskReturn(success=False, summery={"rows inserted": 21, "labels created": 2})

    task = DummyTask(DummyContext())
    ret = task.execute()
    assert ret.success == False
    assert ret.summery == {'labels created': 2, 'rows inserted': 21}


def test_task_group():
    class DummyTask1(Task):
        def run_internal(self, *args, **kwargs):
            return TaskReturn(success=False, summery={"rows inserted": 2, "labels created": 2})

        def abort_on_fail(self) -> bool:
            return False

    class DummyTask2(Task):
        def run_internal(self, *args, **kwargs):
            return TaskReturn(success=True, summery={"rows inserted": 3, "labels created": 3, "foo": 4})

    group = TaskGroup(DummyContext(), [DummyTask1(DummyContext()), DummyTask2(DummyContext())], "test-group")
    ret = group.execute()

    assert ret.success == False
    assert ret.summery == {"foo": 4, "rows inserted": 5, "labels created": 5}


def test_parallel_task_group(etl_context):
    class StartRecordingReporter(DummyReporter):

        def started_task(self, task: Task) -> Task:
            task.start_time = datetime.now()
            return task

        def finished_task(self, task, success: bool, summery: dict, error: str = None) -> Task:
            task.end_time = datetime.now()
            return task

    etl_context.reporter = StartRecordingReporter()

    query = """
            UNWIND range(1, 1_000) AS id
            CALL (id) {{
                MERGE (prev:{label} {{id: id-1}})
                CREATE (d:{label} {{id: id}})
                CREATE (prev)-[:NEXT]->(d)
            }} IN TRANSACTIONS OF 5000 ROWS
            """

    class DummyTask1(ExecuteCypherTask):

        def __init__(self, context):
            super().__init__(context)

        def _query(self) -> str | list[str]:
            return query.format(label="DummyTask1")

    class DummyTask2(ExecuteCypherTask):

        def __init__(self, context):
            super().__init__(context)

        def _query(self) -> str | list[str]:
            return query.format(label="DummyTask2")

    task1 = DummyTask1(etl_context)
    task2 = DummyTask2(etl_context)
    task_group = ParallelTaskGroup(etl_context, [task1, task2], "test-group")

    etl_context.reporter.register_tasks(task_group)
    ret = task_group.execute()
    assert ret.success == True
    assert ret.summery["labels_added"] == 2002
    assert ret.summery["properties_set"] == 2002
    assert ret.summery["relationships_created"] == 2000

    assert task1.end_time > task2.start_time

    etl_context.reporter = DummyReporter()


def test_kwargs_passing():
    class DummyTask(Task):
        def __init__(self, context):
            super().__init__(context)
            self.test = {}

        def run_internal(self, *args, **kwargs):
            self.test = kwargs
            return TaskReturn(success=False, summery=kwargs)

    expected = {"a": 2, "b": 3, "c": 4}
    d1 = DummyTask(DummyContext())
    d1.execute(**expected)
    assert d1.test == expected

    d2 = DummyTask(DummyContext())
    g = TaskGroup(DummyContext(), [d2], "test-group")
    ret = g.execute(**expected)
    assert d2.test == expected
