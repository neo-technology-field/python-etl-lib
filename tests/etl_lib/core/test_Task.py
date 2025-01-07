from etl_lib.core.Task import Task, TaskReturn, TaskGroup
from etl_lib.core.utils import merge_summery
from test_utils.utils import DummyContext


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

    group = TaskGroup(DummyContext(), [DummyTask1(DummyContext()), DummyTask2(DummyContext())], "rest-group")
    ret = group.execute()

    assert ret.success == True
    assert ret.summery == {"foo": 4, "rows inserted": 5, "labels created": 5}
