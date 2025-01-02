from etl_lib.core.Task import merge_summery, Task, InternalResult, TaskGroup


def test_merge_summery():
    merged = merge_summery({"a": 2, "b": 3}, {"b": 2, "c": 4})

    assert merged == {"a": 2, "b": 5, "c": 4}


def test_simple_task():

    class DummyTask(Task):
        def run_internal(self, *args, **kwargs):
            return InternalResult(success=False, summery={"rows inserted": 21, "labels created": 2})

    task = DummyTask(None)
    ret = task.execute()
    assert ret.success == False
    assert ret.summery == {'labels created': 2, 'rows inserted': 21}


def test_task_group():
    class DummyTask1(Task):
        def run_internal(self, *args, **kwargs):
            return InternalResult(success=False, summery={"rows inserted": 2, "labels created": 2})
        def abort_on_fail(self) -> bool:
            return False

    class DummyTask2(Task):
        def run_internal(self, *args, **kwargs):
            return InternalResult(success=True, summery={"rows inserted": 3, "labels created": 3, "foo": 4})

    class DummyGroup(TaskGroup):
        def task_definitions(self):
            return [DummyTask1, DummyTask2]

    group = DummyGroup(None)
    ret = group.execute()

    assert ret.success == True
    assert ret.summery == {"foo": 4, "rows inserted": 5, "labels created": 5}
