from importlib.util import find_spec


def main() -> None:
    """Check the installed distribution without source-path or dev-extra assistance."""
    for package in ("sqlalchemy", "graphdatascience", "pytest", "_pytest"):
        if find_spec(package) is not None:
            raise RuntimeError(f"Base-install smoke environment unexpectedly contains {package}")

    from etl_lib.core.ETLContext import SQLContext
    from etl_lib.test_utils import utils

    if utils.MockETLContext is None or utils.MockNeo4jContext is None:
        raise RuntimeError("Base testing helpers must be importable")
    try:
        SQLContext("sqlite://")
    except ImportError as exc:
        if "neo4j-etl-lib[sql]" not in str(exc):
            raise
    else:
        raise RuntimeError("SQL construction must require its extra")


if __name__ == "__main__":
    main()
