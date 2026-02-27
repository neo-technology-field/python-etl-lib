# Neo4j ETL Toolbox

A robust Python library of building blocks to assemble efficient, scalable ETL pipelines for Neo4j.

It simplifies the process of moving data from SQL, CSV, and Parquet sources into Neo4j by handling common concerns like batching, parallelism, logging, and error handling.

## Key Features

*   **Task-Based Architecture**: Compose pipelines from reusable units of work.
*   **Parallel Loading**: Optimized strategies for high-performance loading without locking issues.
*   **Data Validation**: Integrated Pydantic support for ensuring data quality before loading.
*   **Detailed Reporting**: Built-in tracking of execution time and row counts.
*   **Flexible Sources**: Support for SQL (via SQLAlchemy), CSV, Neo4j and Parquet (via PyArrow).

## Parallel Loading Example

The library provides specialized tasks for parallel data loading. By using a "mix-and-batch" strategy, it can load relationships in parallel while minimizing deadlocks.

Here is an example of defining a parallel CSV loader task (taken from the `examples/nyc-taxi` project):

```python
from pathlib import Path
from etl_lib.core.ETLContext import ETLContext
from etl_lib.core.SplittingBatchProcessor import dict_id_extractor
from etl_lib.task.data_loading.ParallelCSVLoad2Neo4jTask import ParallelCSVLoad2Neo4jTask
from model.trip import Trip # Your Pydantic model

class LoadTripsParallelTask(ParallelCSVLoad2Neo4jTask):
    def __init__(self, context: ETLContext, csv_path: Path):
        super().__init__(
            context,
            file=csv_path,
            model=Trip,
            error_file=Path('errors_parallel.json'),
            batch_size=5000,
            max_workers=10
        )

    def _query(self):
        return """
            UNWIND $batch AS row
            MATCH (pu:Location {id: row.pu_location})
            MATCH (do:Location {id: row.do_location})
            CREATE (t:Trip {
              id: randomUUID(),
              pickup_datetime: row.pickup_datetime,
              dropoff_datetime: row.dropoff_datetime,
              ...
            })
            CREATE (t)-[:STARTED_AT]->(pu)
            CREATE (t)-[:ENDED_AT]->(do)
        """

    def _id_extractor(self):
        # Defines how to route rows to avoid locking on start/end nodes
        return dict_id_extractor(table_size=10, start_key='pu_location', end_key='do_location')
```

## Documentation & Examples

Complete documentation can be found on https://neo-technology-field.github.io/python-etl-lib/index.html

See the examples directory for complete projects:
*   [GTFS Example](https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/gtfs)
*   [MusicBrainz Example](https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/musicbrainz)

## Installation

The library can be installed via:

```bash
pip install neo4j-etl-lib
```

## System Dependencies

Some components or documentation tools require additional system-level packages.

### Graphviz
If you are building the documentation locally and want to generate diagrams (e.g., using `make docs`), you need Graphviz installed.

**Debian/Ubuntu:**
```bash
sudo apt install graphviz
```

**Fedora/RHEL/CentOS:**
```bash
sudo dnf install graphviz
```

**Arch Linux / CachyOS:**
```bash
sudo pacman -S graphviz
```
