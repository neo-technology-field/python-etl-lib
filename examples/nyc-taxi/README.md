# NYC Yellow Taxi ETL Example

## Overview
This example project demonstrates how to use the **etl-lib** toolkit to load the NYC Yellow Taxi trip dataset (CSV format) into a Neo4j graph database. It includes two ETL pipelines:
1. **Sequential** loading (single-threaded)
2. **Parallel** loading (multithreaded using `ParallelBatchProcessor`)

## Data Source
The project expects the NYC Yellow Taxi trip data in **CSV** format, such as:
```
VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,PULocationID,DOLocationID,total_amount
1,2023-01-01 00:05:00,2023-01-01 00:15:00,1,2.5,138,239,12.5
2,2023-01-01 01:20:00,2023-01-01 01:35:00,2,3.8,132,41,15.7
```

## Data Model
The graph schema includes:
- **Trip** nodes with properties:
  - `pickup_datetime` (`datetime`)
  - `dropoff_datetime` (`datetime`)
  - `passenger_count` (`int`)
  - `trip_distance` (`float`)
  - `total_amount` (`float`)
  - `vendor` (`int`)
- **Location** nodes (taxi zone) with `id` (int)
- Relationships:
  - `(Trip)-[:STARTED_AT]->(Location)`
  - `(Trip)-[:ENDED_AT]->(Location)`

Yes, normally one would model the vendor as it own node, but since there are very few vendors in the source data, this would cancel out the parallel loading effect. In the end, this is just an example showcase.

## Setup and Running

1. **Environment**  
   Copy `.env.sample` to `.env` and fill in your Neo4j connection details:
   ```bash
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USERNAME=neo4j
   NEO4J_PASSWORD=your_password
   DATABASE_NAME=neo4j
   ```

2. **Install dependencies**  
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install .
   ```

3. **Run ETL**  
   - Sequential load:
     ```bash
     python src/main.py sequential data/sample_yellow_tripdata.csv
     ```
   - Parallel load:
     ```bash
     python src/main.py parallel data/sample_yellow_tripdata.csv
     ```
