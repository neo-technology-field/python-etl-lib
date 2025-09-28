
from pathlib import Path

from etl_lib.core.ETLContext import ETLContext
from etl_lib.task.data_loading.CSVLoad2Neo4jTask import CSVLoad2Neo4jTask

from model.trip import Trip


class LoadTripsSequentialTask(CSVLoad2Neo4jTask):
    """Sequential loader for NYC Yellow Taxi CSV data."""

    def __init__(self, context: ETLContext, csv_path: Path):
        super().__init__(context, Trip, csv_path, batch_size=5000)

    def _query(self):
        return """
        UNWIND $batch AS row
          MERGE (pu:Location {id: row.pu_location})
          MERGE (do:Location {id: row.do_location})
          CREATE (t:Trip {
            id: randomUUID(),
            pickup_datetime: row.pickup_datetime,
            dropoff_datetime: row.dropoff_datetime,
            passenger_count: row.passenger_count,
            trip_distance: row.trip_distance,
            total_amount: row.total_amount,
            vendor: row.vendor_id
          })
          MERGE (t)-[:STARTED_AT]->(pu)
          MERGE (t)-[:ENDED_AT]->(do)
        """
