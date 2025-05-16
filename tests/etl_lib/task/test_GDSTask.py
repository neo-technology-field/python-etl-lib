from etl_lib.core.Task import TaskReturn
from etl_lib.task.GDSTask import GDSTask, transform_dict
from etl_lib.test_utils.utils import check_property_exists


def test_GDSTask(etl_context):

    def gds_fun(etl_context):
        with etl_context.neo4j.gds as gds:
            gds.graph.drop("neo4j-offices", failIfMissing=False)
            g_office, project_result = gds.graph.project("neo4j-offices", "City", "FLY_TO")
            mutate_result = gds.pageRank.write(g_office, tolerance=0.5, writeProperty="rank")
            return TaskReturn(success=True, summery=transform_dict(mutate_result.to_dict()))

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
    assert task_return.summery is not None
    assert task_return.summery["didConverge"] is True
    assert task_return.summery["nodePropertiesWritten"] == 3

    assert check_property_exists(etl_context.neo4j.driver, "City", "rank")



