from datetime import date

from neo4j.spatial import WGS84Point
from pydantic import BaseModel, Field, field_validator

from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask
from etl_lib.test_utils.utils import get_test_file, get_node_count


def convert_geo(geo_string) -> WGS84Point:
    a = geo_string.split('_')
    WGS84Point((a[0], a[1]))
    return WGS84Point((a[0], a[1]))


class Customer(BaseModel):
    customer_id: str = Field(alias="customerId")
    first_name: str = Field(alias="surname")
    last_name: str = Field(alias="familyName")
    email: str = Field()
    birth_date: date = Field(alias="birthDate")
    country: str = Field(alias="countryCode")
    city: str = Field(alias="cityName")
    street: str = Field(alias="streetName")
    house_number: str = Field(alias="houseNumber")
    turnover: float = Field(alias="turnover")
    coordinates: WGS84Point = Field(alias="gpsCoordinates")

    @field_validator('coordinates', mode='plain')
    @classmethod
    def to_coordinate(cls, value: str) -> WGS84Point | None:
        if value in ["-73.853500_147.217685", "-27.013984_138.844468"]:
            return None
        splits = value.split('_')
        if len(splits) != 2:
            raise ValueError(f"Invalid coordinates format: {value}")
        WGS84Point((float(splits[0]), float(splits[1])))
        return WGS84Point((float(splits[0]), float(splits[1])))


class CustomerLoadTask(CSVLoad2Neo4jTask):

    def __init__(self, context):
        super().__init__(context, Customer, file=get_test_file("customers.csv"), batch_size=20)

    def _query(self):
        return """
        UNWIND $batch AS r
        MERGE (c:Customer {id: r.customer_id})
            SET c.firstName = r.first_name,
            c.lastName = r.last_name,
            c.birthDate = r.birth_date
        MERGE (email:EMailAddress {email: r.email})
        MERGE (c)-[:HAS_EMAIL]->(email)
        MERGE (country:Country {code: r.country})
        MERGE (city:City {name: r.city})
        MERGE (city)-[:IS_IN]->(country)
        MERGE (streetNbr:StreetNumber {street: r.street, number: r.house_number})
        MERGE (streetNbr)-[:IS_IN]->(city)
        MERGE (c)-[:LIVES_AT]->(streetNbr)
        WITH r, streetNbr
        CALL (r, streetNbr) {
            WITH r, streetNbr WHERE r.coordinates IS NOT NULL
                    MERGE (coord:Coordinate {coordinates: r.coordinates})
        MERGE (streetNbr)-[:LOCATED_AT]->(coord)
        }
        """

    def file_prefix(self):
        pass


def test_load(etl_context, neo4j_driver):
    task = CustomerLoadTask(etl_context)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result is not None
    assert result.success is True
    assert result.error is None
    assert result.summery == {'constraints_added': 0,
                              'constraints_removed': 0,
                              'indexes_added': 0,
                              'indexes_removed': 0,
                              'csv_lines_read': 60,
                              'valid_rows': 59,
                              'invalid_rows': 1,
                              'labels_added': 343,
                              'labels_removed': 0,
                              'nodes_created': 343,
                              'nodes_deleted': 0,
                              'properties_set': 579,
                              'relationships_created': 293,
                              'relationships_deleted': 0
                              }

    # one of the 60 customers is not valid
    assert 59 == get_node_count(neo4j_driver, 'Customer')
    assert 58 == get_node_count(neo4j_driver, 'City')
