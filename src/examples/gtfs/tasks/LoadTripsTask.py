from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from etl_lib.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTasks


class LoadTripsTask(CSVLoad2Neo4jTasks):
    class Trip(BaseModel):
        id: str = Field(alias="trip_id")
        route_id: str
        serviceId: str = Field(alias="service_id")
        headsign: str = Field(alias="trip_headsign")
        direction: int = Field(alias="direction_id")
        shortName: Optional[str] = Field(alias="trip_short_name", default=None)
        blockId: Optional[str] = Field(alias="block_id", default=None)
        wheelchairAccessible: Optional[bool] = Field(alias="wheelchair_accessible", default=None)
        bikesAllowed: Optional[bool] = Field(alias="bikes_allowed", default=None)
        shapeId: str = Field(alias="shape_id")

    def __init__(self, context: ETLContext, file:Path):
        super().__init__(context, LoadTripsTask.Trip, file)

    def _query(self):
        return """ UNWIND $batch AS row
        MATCH (r:Route {id: row.route_id})
        MERGE (t:Trip {id: row.id})
            SET t.serviceId= row.serviceId,
                t.headsign= row.headsign, 
                t.direction= row.direction,
                t.shortName= row.shortName, 
                t.blockId= row.blockId,
                t.wheelchairAccessible= row.wheelchairAccessible,
                t.bikesAllowed= row.bikesAllowed, 
                t.shapeId= row.shapeId
        MERGE (r)<-[:USES]-(t);
        """

    @classmethod
    def file_name(cls):
        return "trips.txt"
