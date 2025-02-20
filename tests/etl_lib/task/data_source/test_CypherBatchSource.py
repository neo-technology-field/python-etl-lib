from etl_lib.data_source.CypherBatchSource import CypherBatchSource


def test_cypher_batch(etl_context):
    query = """
    UNWIND range(1,5) AS i
    RETURN {i: i, string: toString(i), float: toFloat(i)} AS val
    """

    sut = CypherBatchSource(context=etl_context, task=None, query=query)
    result_gen = sut.get_batch(max_batch_size=3)
    data = []
    for result in result_gen:
        data.append(result.chunk)

    assert data == [
        [{"val": {
            "string": "1",
            "float": 1.0,
            "i": 1,
        }},
        {"val": {
            "string": "2",
            "float": 2.0,
            "i": 2,
        }},
        {"val": {
            "string": "3",
            "float": 3.0,
            "i": 3,
        }}],
        [{"val": {
            "string": "4",
            "float": 4.0,
            "i": 4,
        }},
        {"val": {
            "string": "5",
            "float": 5.0,
            "i": 5,
        }}]
    ]

def test_cypher_batch_parameters(etl_context):
    query = """
    UNWIND range(1,2) AS i
    RETURN {i: i, param1: $param1, param2: $param2} AS val
    """

    sut = CypherBatchSource(context=etl_context, task=None, query=query, param1="foo", param2="bar")
    result_gen = sut.get_batch(max_batch_size=10)
    data = []
    for result in result_gen:
        data.append(result.chunk)

    assert data == [
        [
            {"val": {
                "i": 1,
                "param1": "foo",
                "param2": "bar",
            }},
            {"val": {
                "i": 2,
                "param1": "foo",
                "param2": "bar",
            }},
        ]
    ]
