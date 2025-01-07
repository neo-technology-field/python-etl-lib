from datetime import date

from neo4j.spatial import WGS84Point
from pydantic import BaseModel, Field, field_validator

from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTasks
from test_utils.fixtures import etl_context
from test_utils.utils import get_test_file


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


class CustomerLoadTask(CSVLoad2Neo4jTasks):

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


def test_load(etl_context):
    task = CustomerLoadTask(etl_context)
    etl_context.reporter.register_tasks(task)
    task.execute()
