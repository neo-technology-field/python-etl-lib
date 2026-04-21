import csv
from datetime import date
from pathlib import Path

from neo4j.spatial import WGS84Point
from pydantic import BaseModel, Field, field_validator

from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask
from etl_lib.test_utils.utils import get_node_count


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
        super().__init__(context, Path(__file__).parent / "../../../data/customers.csv", model=Customer, batch_size=20)

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


class NoValidationLoadTask(CSVLoad2Neo4jTask):
    def __init__(self, context, file: Path):
        super().__init__(context, file, batch_size=10)

    def _query(self):
        return """
        UNWIND $batch AS r
        MERGE (c:RawCustomer {id: r.customerId})
            SET c.name = r.surname
        """


class TSVLoadTask(CSVLoad2Neo4jTask):
    """Loads a tab-delimited file to verify kwargs forwarding to CSVBatchSource."""

    def __init__(self, context, file: Path):
        super().__init__(context, file, batch_size=10)

    def _query(self):
        return """
        UNWIND $batch AS r
        MERGE (n:TSVRow {string: r.string})
            SET n.integer = r.integer, n.float = r.float
        """


def _write_simple_customer_csv(path: Path):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["customerId", "surname"])
        writer.writeheader()
        writer.writerow({"customerId": "1", "surname": "Alice"})
        writer.writerow({"customerId": "2", "surname": "Bob"})


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


def test_load_without_validation_model(etl_context):
    csv_file = Path(etl_context.env("ETL_ERROR_PATH")) / "raw_customers.csv"
    _write_simple_customer_csv(csv_file)

    task = NoValidationLoadTask(etl_context, csv_file)
    etl_context.reporter.register_tasks(task)
    result = task.execute()

    assert result is not None
    assert result.success is True
    assert result.error is None
    assert result.summery.get("valid_rows") is None
    assert result.summery.get("invalid_rows") is None

    with etl_context.neo4j.session() as sess:
        records = sess.run("MATCH (c:RawCustomer) RETURN c.id AS id, c.name AS name")
        customers = {(r["id"], r["name"]) for r in records}
    assert customers == {("1", "Alice"), ("2", "Bob")}


def test_load_tsv(etl_context, neo4j_driver):
    """kwargs (e.g. delimiter) must be forwarded from run_internal to CSVBatchSource."""
    tsv_file = Path(__file__).parent / "../../../data/tab-no-quotes.csv"
    task = TSVLoadTask(etl_context, tsv_file)
    etl_context.reporter.register_tasks(task)

    result = task.execute(delimiter="\t")

    assert result.success is True
    assert result.summery.get("csv_lines_read") == 3

    with etl_context.neo4j.session() as sess:
        records = sess.run("MATCH (n:TSVRow) RETURN n.string AS s ORDER BY n.string")
        strings = [r["s"] for r in records]
    assert len(strings) == 3
    assert "Hello, World!" in strings
