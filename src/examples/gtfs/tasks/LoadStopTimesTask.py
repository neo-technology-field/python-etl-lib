from pathlib import Path

from pydantic import BaseModel

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTasks


class LoadStopTimesTask(CSVLoad2Neo4jTasks):
    class StopTime(BaseModel):
        trip_id: str
        stop_id: str
        arrival_time: str
        departure_time: str
        stop_sequence: int

    def __init__(self, context: ETLContext, file:Path):
        super().__init__(context, LoadStopTimesTask.StopTime, file)

    def _query(self):
        return """UNWIND $batch AS row
        MATCH (t:Trip {id: row.trip_id})
        MATCH (s:Stop {id: row.stop_id})
        MERGE (t)<-[:BELONGS_TO]-(st:StopTime 
            {
            arrivalTime: row.arrival_time, 
            departureTime: row.departure_time,
            arrivalOffset: duration({hours:toInteger(split(row.arrival_time, ':')[0]), minutes:toInteger(split(row.arrival_time, ':')[1]), seconds:toInteger(split(row.arrival_time, ':')[2])}),
            departureOffset: duration({hours:toInteger(split(row.departure_time, ':')[0]), minutes:toInteger(split(row.departure_time, ':')[1]), seconds:toInteger(split(row.departure_time, ':')[2])}),
            stopSequence: row.stop_sequence
            }
        )-[:STOPS_AT]->(s)
        """

    @classmethod
    def file_name(cls):
        return "stop_times.txt"
