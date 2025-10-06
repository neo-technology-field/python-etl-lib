# Usage locally:
#
# nox -s py → current Python vs latest Neo4j
#
# nox -s pyall → 3.10–3.13 vs latest Neo4j
#
# nox -s neo4j → current Python vs LTS + latest Neo4j (sequential)

import json
import argparse
import nox

PY_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]

LTS_NEO4J = "5.26-enterprise"
LATEST_NEO4J = "2025.09.0-enterprise"
NEO4J_IMAGES = [LTS_NEO4J, LATEST_NEO4J]


def _install_test_stack(session: nox.Session) -> None:
    session.install(".[dev,gds,sql]")


# A) Use the *current* interpreter vs latest Neo4j
@nox.session
def py(session: nox.Session) -> None:
    _install_test_stack(session)
    session.env["NEO4J_TEST_CONTAINER"] = f"neo4j:{LATEST_NEO4J}"
    session.run("pytest", "-q")


# B) Local convenience: run against *all* Python versions in one go (latest Neo4j)
@nox.session(python=PY_VERSIONS)
def pyall(session: nox.Session) -> None:
    _install_test_stack(session)
    session.env["NEO4J_TEST_CONTAINER"] = f"neo4j:{LATEST_NEO4J}"
    session.run("pytest", "-q")


# C) Neo4j variants (single Python)
@nox.session
@nox.parametrize("neo4j_image", NEO4J_IMAGES)
def neo4j(session: nox.Session, neo4j_image: str) -> None:
    _install_test_stack(session)
    session.env["NEO4J_TEST_CONTAINER"] = f"neo4j:{neo4j_image}"
    session.run("pytest", "-q")


# Tiny CLI so CI can read PY_VERSIONS without duplicating them
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-py-versions", action="store_true",
                        help="Print Python versions (JSON) for CI matrix.")
    args = parser.parse_args()
    if args.print_py_versions:
        print(json.dumps(PY_VERSIONS))
