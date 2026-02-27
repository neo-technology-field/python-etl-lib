# Neo4j ETL Toolbox

A Python library of building blocks to assemble etl pipelines.

Complete documentation can be found on https://neo-technology-field.github.io/python-etl-lib/index.html

See https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/gtfs 

or 

https://github.com/neo-technology-field/python-etl-lib/tree/main/examples/musicbrainz

for example projects.


The library can be installed via 

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
