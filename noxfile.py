import nox

LATEST_NEO4J = "2025.09.0-enterprise"
LTS_NEO4J = "5.26-enterprise"

@nox.session(python=["3.10", "3.11", "3.12", "3.13"])
def py(session):
    session.install(".[dev,sql,gds]")
    session.env["NEO4J_TEST_CONTAINER"] = f"neo4j:{LATEST_NEO4J}"
    session.run("pytest", "-q")

@nox.session(python=["3.13"])
@nox.parametrize("neo4j", [LTS_NEO4J, LATEST_NEO4J])
def neo4j(session, neo4j):
    session.install(".[dev,sql,gds]")
    session.env["NEO4J_TEST_CONTAINER"] = f"neo4j:{neo4j}"
    session.run("pytest", "-q")
