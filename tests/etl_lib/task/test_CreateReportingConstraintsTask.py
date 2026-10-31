import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from etl_lib.core.ETLContext import QueryResult
from etl_lib.task.CreateReportingConstraintsTask import CreateReportingConstraintsTask


def test_missing_reporter_database_does_not_open_session() -> None:
    neo4j = MagicMock()
    context = SimpleNamespace(env=lambda key: None, neo4j=neo4j)
    with pytest.raises(RuntimeError, match="REPORTER_DATABASE needs to be set"):
        CreateReportingConstraintsTask(context).run_internal()
    neo4j.session.assert_not_called()


def test_reporter_constraint_uses_configured_database() -> None:
    neo4j = MagicMock()
    neo4j.query_database.return_value = QueryResult([], {"constraints_added": 1})
    context = SimpleNamespace(env=lambda key: "reporting", neo4j=neo4j)
    result = CreateReportingConstraintsTask(context).run_internal()
    neo4j.session.assert_called_once_with("reporting")
    assert result.success
    assert result.summary == {"constraints_added": 1}


def test_missing_reporter_database_with_assertions_disabled() -> None:
    program = """
from types import SimpleNamespace
from unittest.mock import MagicMock
from etl_lib.task.CreateReportingConstraintsTask import CreateReportingConstraintsTask

neo4j = MagicMock()
context = SimpleNamespace(env=lambda key: None, neo4j=neo4j)
try:
    CreateReportingConstraintsTask(context).run_internal()
except RuntimeError as exc:
    if 'REPORTER_DATABASE needs to be set' not in str(exc):
        raise
    if neo4j.session.called:
        raise SystemExit('A missing reporter database must not open a session')
else:
    raise SystemExit('Missing configuration was accepted under python -O')
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    result = subprocess.run(
        [sys.executable, "-O", "-B", "-c", program],
        env=environment, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
