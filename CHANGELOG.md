# Changelog

## [0.0.3]
- first proper build release
- added bulk of documentation
- small fixes, but no new features or breaking changes


## [0.1.0]
- added CypherBatchSource to use Neo4j as start of a Task
- added CSVBatchSink for writing data to csv files

## [0.1.1]
- added optional transformer to CypherBatchSource
- the cli now actually does delete runs

## [0.2.0]
- added support for SQL data sources

## [0.3.0]
- added parallel processing using Mix and Batch approach

## [0.3.1]
- fixed incompatibilities with older python versions
- now actually works from 3.10 onwards just as it claims
- added nox tests to automatically test with Python version 3.10 onwards
- added nox tests to run tests with the Neo4j LTS and latest

## [0.3.2]
- changed most of the parallel processing logic to also work with mono-partite graphs
- minor adjustments
