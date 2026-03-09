from pathlib import Path
from typing import Any

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.ETLContext import QueryResult
from etl_lib.core.ParallelBatchProcessor import ParallelBatchProcessor, ParallelBatchResult
from etl_lib.core.SplittingBatchProcessor import SplittingBatchProcessor
from etl_lib.data_sink.CSVBatchSink import CSVBatchSink
from etl_lib.data_sink.CypherBatchSink import CypherBatchSink
from etl_lib.data_sink.SQLBatchSink import SQLBatchSink
from etl_lib.data_source.CSVBatchSource import CSVBatchSource
from etl_lib.data_source.CypherBatchSource import CypherBatchSource
from etl_lib.data_source.ParquetBatchSource import ParquetBatchSource
from etl_lib.data_source.SQLBatchSource import SQLBatchSource


class RecordingReporter:
    """
    Reporter test double that stores instrumentation events.
    """

    def __init__(self):
        """
        Creates a new recording reporter.
        """
        self.events = []

    def instrument(self, task, event_type: str, payload: dict) -> None:
        """
        Stores one instrumentation event.

        Args:
            task: Task associated with the event.
            event_type: Event type.
            payload: Event payload.
        """
        self.events.append({
            "task": task,
            "event_type": event_type,
            "payload": payload,
        })


class TaskStub:
    """
    Minimal task-like object for instrumentation tests.
    """

    def __init__(self, name: str):
        """
        Creates a new task stub.

        Args:
            name: Task display name.
        """
        self.uuid = f"uuid-{name}"
        self._name = name

    def task_name(self) -> str:
        """
        Returns task name.

        Returns:
            Configured task name.
        """
        return self._name


class ContextStub:
    """
    Minimal context-like object for batch processor tests.
    """

    def __init__(self, reporter: RecordingReporter):
        """
        Creates a new context stub.

        Args:
            reporter: Reporter used by processors.
        """
        self.reporter = reporter
        self.neo4j: Any = None
        self.sql: Any = None


class StaticPredecessor(BatchProcessor):
    """
    Predecessor that yields preconfigured batches.
    """

    def __init__(self, batches: list[BatchResults]):
        """
        Creates a new static predecessor.

        Args:
            batches: Batches to emit in order.
        """
        super().__init__(context=None, task=None, predecessor=None)
        self._batches = batches

    def get_batch(self, max_batch_size: int):
        """
        Yields preconfigured batches.

        Args:
            max_batch_size: Unused.
        """
        yield from self._batches


class PassthroughWorker(BatchProcessor):
    """
    Worker that returns incoming chunk unchanged.
    """

    def get_batch(self, max_batch_size: int):
        """
        Yields one transformed batch.

        Args:
            max_batch_size: Unused.
        """
        upstream = next(self.predecessor.get_batch(max_batch_size))
        yield BatchResults(chunk=upstream.chunk, statistics={"processed": len(upstream.chunk)}, batch_size=len(upstream.chunk))


def _event_payloads(events: list[dict], event_type: str) -> list[dict]:
    """
    Filters event payloads by type.

    Args:
        events: Event list.
        event_type: Type to select.

    Returns:
        Payload list.
    """
    return [event["payload"] for event in events if event["event_type"] == event_type]


def test_splitting_batch_processor_emits_splitter_instrumentation():
    """
    Verifies that splitter flush instrumentation is emitted.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    task = TaskStub("splitter-task")

    predecessor = StaticPredecessor([
        BatchResults(chunk=[(0, 0), (1, 1), (0, 0), (1, 1)], statistics={}, batch_size=4)
    ])
    splitter = SplittingBatchProcessor(
        context=context,
        task=task,
        predecessor=predecessor,
        table_size=2,
        id_extractor=lambda item: item,
    )

    list(splitter.get_batch(2))

    payloads = _event_payloads(reporter.events, "splitter_flush")
    assert len(payloads) >= 1
    assert payloads[0]["wave_size"] >= 1
    assert payloads[0]["emitted_rows"] >= 1
    assert payloads[0]["table_size"] == 2
    assert "bucket_min" in payloads[0]
    assert "bucket_p50" in payloads[0]
    assert "bucket_max" in payloads[0]
    assert "dt_ms" in payloads[0]


def test_parallel_batch_processor_emits_wave_and_bucket_instrumentation():
    """
    Verifies that parallel processor emits per-wave and per-bucket instrumentation.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    task = TaskStub("parallel-task")

    wave = ParallelBatchResult(chunk=[[1, 2], [3]], statistics={}, batch_size=3)
    predecessor = StaticPredecessor([wave])

    processor = ParallelBatchProcessor(
        context=context,
        task=task,
        predecessor=predecessor,
        worker_factory=lambda: PassthroughWorker(context=context),
        max_workers=2,
        prefetch=1,
    )

    next(processor.get_batch(10))

    bucket_payloads = _event_payloads(reporter.events, "bucket_done")
    wave_payloads = _event_payloads(reporter.events, "parallel_wave_done")

    assert len(bucket_payloads) == 2
    assert sum(payload["rows"] for payload in bucket_payloads) == 3
    assert len(wave_payloads) == 1
    assert wave_payloads[0]["rows"] == 3
    assert wave_payloads[0]["max_workers"] == 2
    assert wave_payloads[0]["prefetch"] == 1
    assert "dt_ms" in wave_payloads[0]


class FakeNeo4jSession:
    """
    No-op Neo4j session context manager for tests.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FakeNeo4jContext:
    """
    Neo4j context test double for Cypher sink instrumentation tests.
    """

    def session(self):
        """
        Returns a fake session.
        """
        return FakeNeo4jSession()

    def query_database(self, session, query, **kwargs) -> QueryResult:
        """
        Returns a deterministic query result.

        Args:
            session: Unused.
            query: Unused.
            kwargs: Query parameters.

        Returns:
            Query result with `nodes_created` count.
        """
        batch = kwargs.get("batch", [])
        return QueryResult(data=[], summery={"nodes_created": len(batch)})


def test_cypher_batch_sink_emits_transaction_instrumentation():
    """
    Verifies that Cypher sink emits one instrumentation event per processed batch.
    """
    reporter = RecordingReporter()
    task = TaskStub("cypher-sink-task")

    context = ContextStub(reporter)
    context.neo4j = FakeNeo4jContext()

    predecessor = StaticPredecessor([
        BatchResults(chunk=[{"i": 1}, {"i": 2}], statistics={}, batch_size=2),
        BatchResults(chunk=[{"i": 3}], statistics={}, batch_size=1),
    ])

    sink = CypherBatchSink(context=context, task=task, predecessor=predecessor, query="RETURN 1")
    list(sink.get_batch(10))

    payloads = _event_payloads(reporter.events, "cypher_tx_done")
    assert len(payloads) == 2
    assert [payload["rows"] for payload in payloads] == [2, 1]
    assert all("dt_ms" in payload for payload in payloads)


def test_csv_source_and_sink_emit_instrumentation(tmp_path: Path):
    """
    Verifies CSV source and CSV sink instrumentation events.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    task = TaskStub("csv-task")

    source_file = tmp_path / "source.csv"
    source_file.write_text("start,end\n1,2\n3,4\n", encoding="utf-8")

    source = CSVBatchSource(csv_file=source_file, context=context, task=task)
    source_batches = list(source.get_batch(1))
    assert len(source_batches) == 2

    sink_file = tmp_path / "out.csv"
    sink = CSVBatchSink(context=context, task=task, predecessor=StaticPredecessor(source_batches), file_path=sink_file)
    list(sink.get_batch(10))

    read_payloads = _event_payloads(reporter.events, "csv_read_batch")
    write_payloads = _event_payloads(reporter.events, "csv_write_batch")

    assert len(read_payloads) == 2
    assert [payload["rows"] for payload in read_payloads] == [1, 1]
    assert len(write_payloads) == 2
    assert [payload["rows"] for payload in write_payloads] == [1, 1]


class FakeResultProxy:
    """
    SQL result proxy test double.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new result proxy.

        Args:
            rows: Rows to return.
        """
        self._rows = rows

    def mappings(self):
        """
        Returns row iterator.
        """
        return iter(self._rows)


class FakeSqlConnection:
    """
    SQLAlchemy-like connection test double.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new fake connection.

        Args:
            rows: Rows returned by query execution.
        """
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execution_options(self, **kwargs):
        """
        Returns connection with applied options.

        Args:
            kwargs: Unused.
        """
        return self

    def begin(self):
        """
        Returns transaction context manager.

        Returns:
            Current connection object.
        """
        return self

    def execute(self, query, params):
        """
        Executes query and returns test rows.

        Args:
            query: Unused.
            params: Unused.
        """
        return FakeResultProxy(self._rows)


class FakeSqlEngine:
    """
    SQLAlchemy-like engine test double.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new fake engine.

        Args:
            rows: Rows returned by query execution.
        """
        self._rows = rows

    def connect(self):
        """
        Returns a fake SQL connection.
        """
        return FakeSqlConnection(self._rows)


def test_sql_source_emits_instrumentation():
    """
    Verifies SQL source emits one instrumentation event per emitted batch.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    context.sql = type("Sql", (), {"engine": FakeSqlEngine([{"id": 1}, {"id": 2}, {"id": 3}])})()
    task = TaskStub("sql-source-task")

    source = SQLBatchSource(context=context, task=task, query="SELECT 1")
    batches = list(source.get_batch(2))
    assert len(batches) == 2

    payloads = _event_payloads(reporter.events, "sql_read_batch")
    assert len(payloads) == 2
    assert [payload["rows"] for payload in payloads] == [2, 1]
    assert all("dt_ms" in payload for payload in payloads)


def test_sql_sink_emits_instrumentation():
    """
    Verifies SQL sink emits one instrumentation event per written batch.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    context.sql = type("Sql", (), {"engine": FakeSqlEngine([])})()
    task = TaskStub("sql-sink-task")

    predecessor = StaticPredecessor([
        BatchResults(chunk=[{"id": 1}, {"id": 2}], statistics={}, batch_size=2),
        BatchResults(chunk=[{"id": 3}], statistics={}, batch_size=1),
    ])

    sink = SQLBatchSink(context=context, task=task, predecessor=predecessor, query="INSERT INTO t VALUES (:id)")
    list(sink.get_batch(2))

    payloads = _event_payloads(reporter.events, "sql_write_batch")
    assert len(payloads) == 2
    assert [payload["rows"] for payload in payloads] == [2, 1]
    assert all("dt_ms" in payload for payload in payloads)


class FakeCypherRecord:
    """
    Neo4j record test double.
    """

    def __init__(self, data: dict):
        """
        Creates a new fake record.

        Args:
            data: Record data.
        """
        self._data = data

    def data(self) -> dict:
        """
        Returns record data.

        Returns:
            Record dictionary.
        """
        return self._data


class FakeCypherTx:
    """
    Neo4j transaction test double.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new fake transaction.

        Args:
            rows: Rows to return.
        """
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def run(self, query: str, **kwargs):
        """
        Returns iterable records.

        Args:
            query: Unused.
            kwargs: Unused.
        """
        return [FakeCypherRecord(row) for row in self._rows]


class FakeCypherSession:
    """
    Neo4j session test double for source tests.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new fake session.

        Args:
            rows: Rows to return.
        """
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def begin_transaction(self):
        """
        Returns a fake transaction.
        """
        return FakeCypherTx(self._rows)


class FakeCypherContext:
    """
    Neo4j context test double for source tests.
    """

    def __init__(self, rows: list[dict]):
        """
        Creates a new fake Neo4j context.

        Args:
            rows: Rows to return.
        """
        self._rows = rows

    def session(self):
        """
        Returns a fake session.
        """
        return FakeCypherSession(self._rows)


def test_cypher_source_emits_instrumentation():
    """
    Verifies Cypher source emits one instrumentation event per emitted batch.
    """
    reporter = RecordingReporter()
    context = ContextStub(reporter)
    context.neo4j = FakeCypherContext([{"i": 1}, {"i": 2}, {"i": 3}])
    task = TaskStub("cypher-source-task")

    source = CypherBatchSource(context=context, task=task, query="RETURN 1")
    batches = list(source.get_batch(2))
    assert len(batches) == 2

    payloads = _event_payloads(reporter.events, "cypher_read_batch")
    assert len(payloads) == 2
    assert [payload["rows"] for payload in payloads] == [2, 1]
    assert all("dt_ms" in payload for payload in payloads)


def test_parquet_source_emits_instrumentation(monkeypatch, tmp_path: Path):
    """
    Verifies Parquet source emits one instrumentation event per emitted batch.
    """

    class FakeParquetBatch:
        """
        Fake pyarrow batch object.
        """

        def __init__(self, rows: list[dict]):
            self._rows = rows

        def to_pylist(self):
            return self._rows

    class FakeParquetFile:
        """
        Fake pyarrow ParquetFile object.
        """

        def __init__(self, file: Path):
            self.file = file

        def iter_batches(self, batch_size: int, **kwargs):
            yield FakeParquetBatch([{"a": 1}, {"a": 2}])
            yield FakeParquetBatch([{"a": 3}])

    class FakePq:
        """
        Fake pyarrow.parquet module.
        """

        ParquetFile = FakeParquetFile

    monkeypatch.setattr("etl_lib.data_source.ParquetBatchSource.pq", FakePq)

    reporter = RecordingReporter()
    context = ContextStub(reporter)
    task = TaskStub("parquet-source-task")

    source = ParquetBatchSource(file=tmp_path / "in.parquet", context=context, task=task)
    batches = list(source.get_batch(2))
    assert len(batches) == 2

    payloads = _event_payloads(reporter.events, "parquet_read_batch")
    assert len(payloads) == 2
    assert [payload["rows"] for payload in payloads] == [2, 1]
    assert all("dt_ms" in payload for payload in payloads)
