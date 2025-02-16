import uuid
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, HttpUrl, EmailStr, Field, field_validator

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask


class LoadAgenciesTask(CSVLoad2Neo4jTask):
    class Agency(BaseModel):
        id: Optional[str] = Field(alias="agency_id", default_factory=lambda: str(uuid.uuid4()))
        name: str = Field(..., alias="agency_name")
        url: HttpUrl = Field(..., alias="agency_url")
        timezone: str = Field(..., alias="agency_timezone")
        lang: Optional[str] = Field(None, alias="agency_lang")
        phone: Optional[str] = Field(None, alias="agency_phone")
        fare_url: Optional[HttpUrl] = Field(None, alias="agency_fare_url")
        email: Optional[EmailStr] = Field(None, alias="agency_email")

        @field_validator("id", mode="before")
        @classmethod
        def ensure_id(cls, v):
            return v or str(uuid.uuid4())

        class Config:
            json_encoders = {
                HttpUrl: lambda v: str(v)
            }

    def __init__(self, context: ETLContext, file: Path):
        super().__init__(context, LoadAgenciesTask.Agency, file)

    def task_name(self) -> str:
        return f"{self.__class__.__name__}('{self.file}')"

    def _query(self):
        return """ UNWIND $batch AS row
        MERGE (a:Agency {id: row.id})
            SET a.name= row.name, 
            a.url = row.url, 
            a.timezone = row.timezone, 
            a.lang = row.lang,
            a.phone = row.phone, 
            a.fare_url = row.fare_url, 
            a.email = row.email
        """

    @classmethod
    def file_name(cls):
        return "agency.txt"
