import pytest

from etl_lib.core.Task import TaskReturn
from etl_lib.task.GDSTask import GDSTask, transform_dict
from etl_lib.test_utils.utils import check_property_exists


def test_GDSTask(etl_context):
    if getattr(etl_context.neo4j, "gds", None) is None:
        pytest.skip("graphdatascience not available on this Python version")

    def gds_fun(etl_context):
        gds =  etl_context.neo4j.gds
        gds.graph.drop("neo4j-offices", fail_if_missing=False)
        g_office, project_result = gds.graph.project.native("neo4j-offices", "City", "FLY_TO")
        write_result = gds.page_rank.write(g_office, tolerance=0.5, write_property="rank")
        return TaskReturn(success=True, summary=transform_dict(write_result.model_dump(by_alias=True)))

    with etl_context.neo4j.session() as session:
        session.run("""
         CREATE
          (m: City {name: "Malmö"}),
          (l: City {name: "London"}),
          (s: City {name: "San Mateo"}),
          (m)-[:FLY_TO]->(l),
          (l)-[:FLY_TO]->(m),
          (l)-[:FLY_TO]->(s),
          (s)-[:FLY_TO]->(l)
        """)

    task = GDSTask(etl_context, gds_fun)
    task_return = task.execute()
    assert task_return.success is True
    assert task_return.summary is not None
    assert task_return.summary["didConverge"] is True
    assert task_return.summary["nodePropertiesWritten"] == 3

    assert check_property_exists(etl_context.neo4j.driver, "City", "rank")
