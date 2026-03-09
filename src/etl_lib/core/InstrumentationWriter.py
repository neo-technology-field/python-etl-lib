import csv
import threading
from pathlib import Path
from typing import Any


class InstrumentationWriter:
    """
    Base writer for instrumentation events.

    Implementations decide where and how events are persisted.
    """

    enabled: bool = False

    def write(self, event: dict[str, Any]) -> None:
        """
        Persist one instrumentation event.

        Args:
            event: Event payload to persist.
        """
        raise NotImplementedError


class NoopInstrumentationWriter(InstrumentationWriter):
    """
    Disabled writer implementation.
    """

    enabled = False

    def write(self, event: dict[str, Any]) -> None:
        """
        Ignore instrumentation events.

        Args:
            event: Event payload to persist.
        """
        return


class CsvInstrumentationWriter(InstrumentationWriter):
    """
    Writes instrumentation events to CSV.

    The writer is thread-safe and appends rows to the configured file.
    """

    enabled = True

    def __init__(self, path: Path, sample_every: int = 1):
        """
        Creates a new CSV instrumentation writer.

        Args:
            path: Target CSV path.
            sample_every: Writes every Nth event. Values < 1 are treated as 1.
        """
        self.path = path
        self.sample_every = max(1, int(sample_every))
        self._lock = threading.Lock()
        self._event_counter = 0
        self._header_written = self.path.exists() and self.path.stat().st_size > 0
        self._field_names = [
            "ts",
            "run_id",
            "event_type",
            "task_uuid",
            "task_name",
            "rows",
            "success",
            "error",
            "batch_size",
            "wave_size",
            "buckets",
            "max_workers",
            "prefetch",
            "dt_ms",
            "buffered_before",
            "bucket_min",
            "bucket_p50",
            "bucket_max",
            "table_size",
            "emitted_rows",
            "queue_depth",
        ]

    def write(self, event: dict[str, Any]) -> None:
        """
        Appends one event row to the CSV file.

        Args:
            event: Event payload to persist.
        """
        with self._lock:
            self._event_counter += 1
            if self._event_counter % self.sample_every != 0:
                return

            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=self._field_names, extrasaction="ignore")
                if not self._header_written:
                    writer.writeheader()
                    self._header_written = True
                writer.writerow(event)


def create_instrumentation_writer(env_vars: dict) -> InstrumentationWriter:
    """
    Creates an instrumentation writer from environment values.

    Supported modes via `ETL_LIB_INSTRUMENT`:
    - `none` (default)
    - `csv`

    Args:
        env_vars: Environment dictionary.

    Returns:
        A configured instrumentation writer.
    """
    mode = (env_vars.get("ETL_LIB_INSTRUMENT") or "none").lower()
    if mode == "csv":
        path = Path(env_vars.get("ETL_LIB_INSTRUMENT_CSV_PATH") or "etl_instrumentation.csv")
        sample_every = int(env_vars.get("ETL_LIB_INSTRUMENT_SAMPLE") or 1)
        return CsvInstrumentationWriter(path=path, sample_every=sample_every)
    return NoopInstrumentationWriter()
