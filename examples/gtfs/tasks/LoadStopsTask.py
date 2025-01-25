from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTasks


class LoadStopsTask(CSVLoad2Neo4jTasks):
    class Stop(BaseModel):
        id: str = Field(alias="stop_id")
        name: str = Field(alias="stop_name")
        latitude: float = Field(alias="stop_lat")
        longitude: float = Field(alias="stop_lon")
        platform_code: Optional[str] = None
        parent_station: Optional[str] = None
        type: Optional[str] = Field(alias="location_type", default=None)
        timezone: Optional[str] = Field(alias="stop_timezone", default=None)
        code: Optional[str] = Field(alias="stop_code", default=None)

    def __init__(self, context: ETLContext, file: Path):
        super().__init__(context, LoadStopsTask.Stop, file)

    def task_name(self) -> str:
        return f"{self.__class__.__name__}('{self.file}')"

    def _query(self):
        return """UNWIND $batch AS row
        MERGE (s:Stop {id: row.id})
            SET s.name = row.name, 
                s.location= point({latitude: row.latitude, longitude: row.longitude}),
                s.platformCode= row.platform_code, 
                s.parentStation= row.parent_station, 
                s.type= row.type,
                s.timezone= row.timezone, 
                s.code= row.code
        """

    @classmethod
    def file_name(cls):
        return "stops.txt"
