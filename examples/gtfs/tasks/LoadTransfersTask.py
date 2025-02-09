from pathlib import Path

from pydantic import BaseModel, Field

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask


class LoadTransfersTask(CSVLoad2Neo4jTask):
    class Transfer(BaseModel):
        from_stop_id: str
        minTransferTime: int = Field(gt=0, alias="min_transfer_time", default=0)

    def __init__(self, context: ETLContext, file:Path):
        super().__init__(context, LoadTransfersTask.Transfer, file)

    def _query(self):
        return """UNWIND $batch AS row
        MATCH (s:Stop {id:row.from_stop_id}) 
            SET s.minTransferTime = duration({seconds: row.minTransferTime});
        """

    @classmethod
    def file_name(cls):
        return "transfers.txt"
