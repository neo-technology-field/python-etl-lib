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

### Podman + Testcontainers (Linux)

On some rootless Podman setups, Testcontainers may fail with bridge/network errors (for example: `netavark ... create bridge ... Operation not supported`).
In that case, run tests against the rootful Podman socket.

1) Enable rootful Podman API socket:

```bash
sudo systemctl enable --now podman.socket
```

2) Grant non-root access to the rootful socket:

```bash
sudo groupadd -f podman
sudo usermod -aG podman "$USER"
sudo mkdir -p /etc/systemd/system/podman.socket.d
sudo tee /etc/systemd/system/podman.socket.d/override.conf >/dev/null <<'EOF'
[Socket]
SocketGroup=podman
SocketMode=0660
DirectoryMode=0755
EOF
sudo systemctl daemon-reload
sudo systemctl restart podman.socket
```

3) Ensure `/run/podman` remains traversable after reboot (some distros recreate it with restrictive mode):

```bash
sudo tee /etc/tmpfiles.d/podman.conf >/dev/null <<'EOF'
d /run/podman 0755 root root -
EOF
sudo systemd-tmpfiles --create /etc/tmpfiles.d/podman.conf
sudo systemctl restart podman.socket
```

4) Open a new shell/login (or run `newgrp podman`), then configure Testcontainers env:

```bash
export DOCKER_HOST=unix:///run/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
```

If container startup still fails with bridge/network errors, force a different network mode for Testcontainers-created containers:

```bash
export TESTCONTAINERS_NETWORK_MODE=pasta
```

5) Optional but useful on slow networks: pre-pull images into the rootful Podman store:

```bash
sudo podman pull docker.io/library/neo4j:5.26.0-enterprise
sudo podman pull docker.io/library/postgres:15
sudo podman pull docker.io/testcontainers/ryuk:0.8.1
```

6) Run tests:

```bash
pytest
```

Notes:

- Rootless and rootful Podman use separate image stores, so images may appear duplicated.
- You can inspect rootful images with `sudo podman image ls`.

To return to rootless defaults in the current shell:

```bash
unset DOCKER_HOST
unset TESTCONTAINERS_RYUK_DISABLED
unset TESTCONTAINERS_NETWORK_MODE
```
