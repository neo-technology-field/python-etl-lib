from pathlib import Path

from pydantic import BaseModel, Field

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask


class LoadAgenciesTask(CSVLoad2Neo4jTask):
    class Agency(BaseModel):
        id: str = Field(alias="agency_id", default="generic")
        name: str = Field(alias="agency_name")
        url: str = Field(alias="agency_url")
        timezone: str = Field(alias="agency_timezone")
        lang: str = Field(alias="agency_lang")

    def __init__(self, context: ETLContext, file:Path):
        super().__init__(context, LoadAgenciesTask.Agency, file)

    def task_name(self) -> str:
        return f"{self.__class__.__name__}('{self.file}')"

    def _query(self):
        return """ UNWIND $batch AS row
        MERGE (a:Agency {id: row.id})
            SET a.name= row.name, 
            a.url= row.url, 
            a.timezone= row.timezone, 
            a.lang= row.lang
        """

    @classmethod
    def file_name(cls):
        return "agency.txt"
