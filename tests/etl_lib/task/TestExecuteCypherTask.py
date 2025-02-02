from etl_lib.task.ExecuteCypherTask import ExecuteCypherTask


def test_execute_cypher_task(etl_context):

    class TestExecuteCypherTask(ExecuteCypherTask):

        def __init__(self):
            super().__init__(etl_context)

        def _query(self) -> str | list[str]:
            return """
            CREATE (:Test {id:1})-[:TEST]->(:Test {test: $testParam}) 
            """

    task = TestExecuteCypherTask()
    res = task.execute(testParam='testParamValue')
    assert res is not None
    assert res.success is True
    assert res.error is None
    stats = {key: value for key, value in res.summery.items() if value != 0}
    assert stats == {"nodes_created": 2, "relationships_created": 1, "properties_set": 2, "labels_added": 2}


def test_execute_cypher_task_with_error(etl_context):
    class TestExecuteCypherTask(ExecuteCypherTask):

        def __init__(self):
            super().__init__(etl_context)

        def _query(self) -> str | list[str]:
            return """
            INVALID CYPHER
            """

    task = TestExecuteCypherTask()
    res = task.execute()
    assert res is not None
    assert res.success is False
    assert res.error is not None
