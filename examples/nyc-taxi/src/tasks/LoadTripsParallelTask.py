from pathlib import Path

from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.SplittingBatchProcessor import dict_id_extractor
from etl_lib.task.data_loading.ParallelCSVLoad2Neo4jTask import ParallelCSVLoad2Neo4jTask

from model.trip import Trip


class LoadTripsParallelTask(ParallelCSVLoad2Neo4jTask):
    """Parallel loader for NYC Yellow Taxi CSV data."""

    def __init__(self, context: ETLContext, csv_path: Path):
        super().__init__(context, file=csv_path, model=Trip, error_file=Path('errors_parallel.json'), batch_size=5000,
                         max_workers=10)
        self.csv_path = csv_path

    def _query(self):
        return """
            UNWIND $batch AS row
            WITH row,
                 CASE WHEN row.pu_location <= row.do_location THEN row.pu_location ELSE row.do_location END AS lowId,
                 CASE WHEN row.pu_location <= row.do_location THEN row.do_location ELSE row.pu_location END AS highId
            MERGE (low:Location {id: lowId})
            MERGE (high:Location {id: highId})
            WITH row, low, high,
                 CASE WHEN row.pu_location <= row.do_location THEN low ELSE high END AS pu,
                 CASE WHEN row.pu_location <= row.do_location THEN high ELSE low END AS do
            CREATE (t:Trip {
              id: randomUUID(),
              pickup_datetime: row.pickup_datetime,
              dropoff_datetime: row.dropoff_datetime,
              passenger_count: row.passenger_count,
              trip_distance: row.trip_distance,
              total_amount: row.total_amount,
              vendor: row.vendor_id
            })
            CREATE (t)-[:STARTED_AT]->(pu)
            CREATE (t)-[:ENDED_AT]->(do)
        """

    def _id_extractor(self):
        return dict_id_extractor(table_size=10, start_key='pu_location', end_key='do_location')
