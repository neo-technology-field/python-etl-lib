import csv
from pathlib import Path

from etl_lib.core.InstrumentationWriter import create_instrumentation_writer
from etl_lib.core.ProgressReporter import ProgressReporter
from etl_lib.core.Task import Task, TaskReturn


class EnvOnlyContext:
    """
    Minimal context implementation used for instrumentation tests.
    """

    def __init__(self, env_vars: dict):
        """
        Creates a new test context.

        Args:
            env_vars: Environment values returned by :func:`~env`.
        """
        self._env_vars = env_vars
        self.instrumentation_writer = create_instrumentation_writer(env_vars)

    def env(self, key: str):
        """
        Returns a configured environment value.

        Args:
            key: Environment key.

        Returns:
            Configured value or `None`.
        """
        return self._env_vars.get(key)


class DummyTask(Task):
    """
    Simple task implementation used for reporter tests.
    """

    def run_internal(self, **kwargs) -> TaskReturn:
        """
        Returns a successful task result.

        Args:
            kwargs: Unused task parameters.

        Returns:
            Successful empty result.
        """
        return TaskReturn(success=True, summery={})


def _read_rows(path: Path) -> list[dict]:
    """
    Reads instrumentation CSV rows.

    Args:
        path: CSV path.

    Returns:
        Parsed rows.
    """
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_csv_instrumentation_contains_task_name_and_run_id(tmp_path):
    """
    Verifies CSV instrumentation rows contain required task and run metadata.
    """
    path = tmp_path / "instrumentation.csv"
    context = EnvOnlyContext({
        "ETL_LIB_INSTRUMENT": "csv",
        "ETL_LIB_INSTRUMENT_CSV_PATH": str(path),
    })
    reporter = ProgressReporter(context)
    task = DummyTask(context)

    reporter.register_tasks(task)
    reporter.instrument(task, "parallel_wave_done", {"rows": 42, "dt_ms": 12.5})

    rows = _read_rows(path)
    assert len(rows) == 1
    assert rows[0]["event_type"] == "parallel_wave_done"
    assert rows[0]["task_name"] == task.task_name()
    assert rows[0]["task_uuid"] == task.uuid
    assert rows[0]["run_id"] == task.uuid
    assert rows[0]["rows"] == "42"


def test_csv_instrumentation_sets_run_id_without_register_tasks(tmp_path):
    """
    Verifies run id is populated even when task registration is skipped.
    """
    path = tmp_path / "instrumentation.csv"
    context = EnvOnlyContext({
        "ETL_LIB_INSTRUMENT": "csv",
        "ETL_LIB_INSTRUMENT_CSV_PATH": str(path),
    })
    reporter = ProgressReporter(context)
    task = DummyTask(context)

    reporter.instrument(task, "bucket_done", {"rows": 3})

    rows = _read_rows(path)
    assert len(rows) == 1
    assert rows[0]["event_type"] == "bucket_done"
    assert rows[0]["run_id"] == task.uuid
    assert rows[0]["task_name"] == task.task_name()
