from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask


class LoadRoutesTask(CSVLoad2Neo4jTask):
    class Route(BaseModel):
        id: str = Field(alias="route_id")
        short_name: str = Field(alias="route_short_name")
        long_name: str = Field(alias="route_long_name")
        type: int = Field(alias="route_type")
        agency_id: str = Field(alias="agency_id")

        @field_validator("agency_id", mode="before")
        @classmethod
        def handle_null_agency_id(cls, value: Optional[str]) -> str:
            return value or "generic"

        @field_validator("type", mode="before")
        @classmethod
        def validate_route_type(cls, value: str) -> int:
            valid_types = {0, 1, 2, 3, 4, 5, 6, 7}
            value = int(value)
            if value not in valid_types:
                raise ValueError(f"Invalid route_type: {value}. Must be one of {valid_types}.")

            return value

    def __init__(self, context: ETLContext, file: Path):
        super().__init__(context, LoadRoutesTask.Route, file)

    def task_name(self) -> str:
        return f"{self.__class__.__name__}('{self.file}')"

    def _query(self):
        return """UNWIND $batch as row
            MATCH (a:Agency {id: row.agency_id})
            MERGE (r:Route {id: row.id}) 
                SET r.shortName= row.short_name,
                    r.longName= row.long_name, 
                    r.type= row.type
            MERGE (a)-[:OPERATES]->(r)
            """

    @classmethod
    def file_name(cls):
        return "routes.txt"
