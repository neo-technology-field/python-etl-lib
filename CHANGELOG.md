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

## [0.3.3]
- hopefully fixes the bug #10 on Windows when writing to a log file that can not handle UTF8
- added `canonical_int_or_str_id_extractor` to handle strings on for bucketing on, minimizing collision chances
- added option to pass connection configuration ala `NEO4J_DRIVER_MAX_CONNECTION_POOL_SIZE=200` 

## [0.3.4]
- added instrumentation to support tuning of parameters for performance
- fixed bug leading to wrong reporting of expected batches in parallel operations
## [0.3.5]
- validation support for CSV loading was not optional (doc claimed so). It is now. This, unfortunately breaks the init parameters.

## [0.3.6]
- fixed `CSVLoad2Neo4jTask.run_internal()` not forwarding `**kwargs` to `CSVBatchSource`, making it impossible to pass reader options such as `delimiter='\t'` for TSV files

## [0.4.0]
- added OAuth2 client credentials token auth for Neo4j connections
- when `NEO4J_CLIENT_ID` is present, the driver authenticates via bearer token instead of username/password
- tokens are fetched and refreshed automatically; works with any standards-compliant OAuth2 provider (Azure AD, Okta, Keycloak, …)

## [0.5.0] — breaking changes
- cleanup, inconsistencies, ... 
### Breaking changes
- `TaskReturn.summery` renamed to `TaskReturn.summary` — update all call sites and attribute access
- `QueryResult.summery` renamed to `QueryResult.summary` — same
- `merge_summery()` in `etl_lib.core.utils` renamed to `merge_summary()`
- `CSVBatchSource.__init__` parameter order changed: `(csv_file, context, task)` → `(context, task, csv_file)`
- `ParquetBatchSource.__init__` parameter order changed: `(file, context, task)` → `(context, task, file)`
- The Neo4j property written by `Neo4jProgressReporter` for task stats is now `$summary` (was `$summery`)

### Bug fixes
- `ETLContext`: `gds_available` was always `False` even when `graphdatascience` was installed — GDS context
  was therefore never initialised
- `TaskGroup.run_internal`: abort-on-failure check used `task_ret == False` which never matched because
  `TaskReturn` has no `__eq__`; the entire abort path was dead code
- `TaskGroup.abort_on_fail`: returned `None` instead of `False` when no child requested abort
- `ClosedLoopBatchProcessor._safe_calculate_count`: returned `expected_rows + batch_size - 1` (almost the
  row count) instead of the correct ceiling-division batch count

### Other
- `assert` guards on `predecessor` in `CypherBatchSink`, `ClosedLoopBatchProcessor`, `ValidationBatchProcessor`,
  `CSVBatchSink`, and `SQLBatchSink` replaced with `ValueError` (assertions are disabled by `-O`)
- Misleading class-level `start_time`/`end_time` annotations removed from `ProgressReporter`
- Docstring typo "Rask" → "Task" in `TaskGroup.__init__`
