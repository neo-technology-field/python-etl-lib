"""Tests for the database configuration surface of the shared CLI group.

The production ``cli`` group carries the real commands, which connect to Neo4j. These
tests build a throwaway group from the production callback and option list, add a
no-op child that captures ``ctx.obj``, and invoke that. Nothing is registered on the
production group, so no command under test can open a connection.
"""

import click
import pytest
from click.testing import CliRunner, Result

from etl_lib.cli.run_tools import cli as production_cli

DATABASE_ENV_VARS = ("NEO4J_DATABASE", "DATABASE_NAME", "REPORTER_DATABASE")


@pytest.fixture(autouse=True)
def clean_database_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove database settings that the parent conftest may have loaded from .env."""
    for name in DATABASE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NEO4J_URI", "bolt://example.invalid:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")


def _build_test_group(captured: dict[str, object]) -> click.Group:
    group = click.Group(name="test-cli")
    group.callback = production_cli.callback
    group.params = list(production_cli.params)
    group.help = production_cli.help

    @click.command(name="capture")
    @click.pass_context
    def capture(ctx: click.Context) -> None:
        captured.update(ctx.obj or {})

    group.add_command(capture)
    return group


def _invoke(args: list[str]) -> tuple[Result, dict[str, object]]:
    captured: dict[str, object] = {}
    result = CliRunner().invoke(_build_test_group(captured), [*args, "capture"])
    return result, captured


def _invoke_with_env(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], args: list[str]
) -> tuple[Result, dict[str, object]]:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return _invoke(args)


def test_defaults_without_database_configuration() -> None:
    result, captured = _invoke([])

    assert result.exit_code == 0, result.output
    assert captured["database_name"] == "neo4j"
    assert captured["reporter_database"] is None


@pytest.mark.parametrize(
    ("env", "expected_database", "expected_reporter"),
    [
        pytest.param({"NEO4J_DATABASE": "etl"}, "etl", None, id="main-database-only"),
        pytest.param({"REPORTER_DATABASE": "etl_reports"}, "neo4j", "etl_reports", id="reporter-database-only"),
        pytest.param(
            {"NEO4J_DATABASE": "etl", "REPORTER_DATABASE": "etl_reports"},
            "etl",
            "etl_reports",
            id="both-databases",
        ),
    ],
)
def test_database_from_environment(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected_database: str, expected_reporter: str | None
) -> None:
    result, captured = _invoke_with_env(monkeypatch, env, [])

    assert result.exit_code == 0, result.output
    assert captured["database_name"] == expected_database
    assert captured["reporter_database"] == expected_reporter


@pytest.mark.parametrize(
    ("args", "expected_database", "expected_reporter"),
    [
        pytest.param(["--database", "cli_db"], "cli_db", None, id="long-option"),
        pytest.param(["-d", "short_db"], "short_db", None, id="short-option"),
        pytest.param(["--reporter-database", "cli_reports"], "neo4j", "cli_reports", id="reporter-option"),
    ],
)
def test_database_from_option(args: list[str], expected_database: str, expected_reporter: str | None) -> None:
    result, captured = _invoke(args)

    assert result.exit_code == 0, result.output
    assert captured["database_name"] == expected_database
    assert captured["reporter_database"] == expected_reporter


@pytest.mark.parametrize(
    ("env", "args", "expected_database", "expected_reporter"),
    [
        pytest.param(
            {"NEO4J_DATABASE": "env_db"},
            ["--database", "cli_db"],
            "cli_db",
            None,
            id="long-option-beats-environment",
        ),
        pytest.param(
            {"NEO4J_DATABASE": "env_db"},
            ["-d", "short_db"],
            "short_db",
            None,
            id="short-option-beats-environment",
        ),
        pytest.param(
            {"REPORTER_DATABASE": "env_reports"},
            ["--reporter-database", "cli_reports"],
            "neo4j",
            "cli_reports",
            id="reporter-option-beats-environment",
        ),
        pytest.param(
            {"NEO4J_DATABASE": "env_db", "REPORTER_DATABASE": "env_reports"},
            ["--database", "cli_db", "--reporter-database", "cli_reports"],
            "cli_db",
            "cli_reports",
            id="both-options-beat-environment",
        ),
    ],
)
def test_options_override_environment(
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
    args: list[str],
    expected_database: str,
    expected_reporter: str | None,
) -> None:
    result, captured = _invoke_with_env(monkeypatch, env, args)

    assert result.exit_code == 0, result.output
    assert captured["database_name"] == expected_database
    assert captured["reporter_database"] == expected_reporter


@pytest.mark.parametrize(
    ("env", "expected_database"),
    [
        pytest.param({"DATABASE_NAME": "legacy_db"}, "neo4j", id="legacy-alone"),
        pytest.param({"DATABASE_NAME": "legacy_db", "NEO4J_DATABASE": "etl"}, "etl", id="legacy-alongside-new-name"),
    ],
)
def test_legacy_database_name_is_ignored(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected_database: str
) -> None:
    result, captured = _invoke_with_env(monkeypatch, env, [])

    assert result.exit_code == 0, result.output
    assert captured["database_name"] == expected_database


def test_removed_database_name_option_is_rejected() -> None:
    result, _ = _invoke(["--database-name", "legacy_db"])

    assert result.exit_code == 2
    assert "No such option" in result.output
    assert "--database-name" in result.output


def test_group_help_shows_current_configuration_surface() -> None:
    result = CliRunner().invoke(_build_test_group({}), ["--help"])

    assert result.exit_code == 0, result.output
    assert "-d, --database TEXT" in result.output
    assert "--reporter-database TEXT" in result.output
    assert "NEO4J_DATABASE" in result.output
    assert "REPORTER_DATABASE" in result.output
    assert "DATABASE_NAME" not in result.output
    assert "--database-name" not in result.output
