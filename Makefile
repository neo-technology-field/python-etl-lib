# Top-level developer Makefile (venv assumed active)
#
# Philosophy:
# - Keep targets composable and predictable.
# - Provide a "help" target that documents everything.
# - Never hide important behavior behind implicit magic; defaults are sensible, overrides are easy.
#
# Assumptions:
# - You already activated a virtualenv (or otherwise have the right python on PATH).
# - Dev dependencies are installed (at minimum pytest + sphinx + nox).
#
# Recommended one-time setup:
#   pip install -e ".[dev,nox]"
#
# Common usage:
#   make test
#   make docs
#   make nox-py
#   make nox-neo4j
#   make nox-pyall
#
# Customization examples:
#   PYTESTOPTS="-k neo4j -vv" make test
#   SPHINXOPTS="-W --keep-going" make docs
#   NOXOPTS="-r -vv" make nox
#   NOX_SESSIONS="py neo4j" make nox
#   DOCS_BUILDDIR=docs/_build make docs

SHELL := /bin/sh
.DEFAULT_GOAL := help

PYTHON ?= python
DOCS_SRCDIR ?= docs
DOCS_BUILDDIR ?= docs/_build

PYTESTOPTS ?=
SPHINXOPTS ?=
NOXOPTS ?=

# Default set of nox sessions to run when invoking `make nox`.
# Overridable: NOX_SESSIONS="py neo4j" make nox
NOX_SESSIONS ?= py

# ----- Internal helpers ------------------------------------------------------

.PHONY: help
help:
	@printf "%s\n" \
	"" \
	"Developer targets (venv assumed active)" \
	"" \
	"Setup" \
	"  deps              Install editable package + dev and nox extras" \
	"" \
	"Testing" \
	"  test              Run pytest" \
	"  test-cov          Run pytest with coverage (pytest-cov must be installed)" \
	"" \
	"Docs (Sphinx)" \
	"  docs              Build HTML docs into $(DOCS_BUILDDIR)/html" \
	"  docs-linkcheck    Run Sphinx linkcheck" \
	"  docs-clean        Remove docs build output" \
	"" \
	"Nox" \
	"  nox-list          List nox sessions" \
	"  nox               Run nox sessions in NOX_SESSIONS (default: $(NOX_SESSIONS))" \
	"  nox-py            Current Python vs latest Neo4j (nox -s py)" \
	"  nox-pyall         Python 3.10–3.13 vs latest Neo4j (nox -s pyall)" \
	"  nox-neo4j         Current Python vs LTS + latest Neo4j (nox -s neo4j)" \
	"" \
	"Cleaning" \
	"  clean             Remove common build/test artifacts" \
	""

.PHONY: deps
deps:
	@$(PYTHON) -m pip install -e ".[dev,nox]"

# ----- Testing ---------------------------------------------------------------

.PHONY: test
test:
	@$(PYTHON) -m pytest $(PYTESTOPTS)

.PHONY: test-cov
test-cov:
	@$(PYTHON) -m pytest --cov --cov-report=term-missing $(PYTESTOPTS)

# ----- Docs ------------------------------------------------------------------

.PHONY: docs
docs:
	@$(PYTHON) -m sphinx -b html $(SPHINXOPTS) "$(DOCS_SRCDIR)" "$(DOCS_BUILDDIR)/html"
	@printf "%s\n" "Built docs: $(DOCS_BUILDDIR)/html/index.html"

.PHONY: docs-linkcheck
docs-linkcheck:
	@$(PYTHON) -m sphinx -b linkcheck $(SPHINXOPTS) "$(DOCS_SRCDIR)" "$(DOCS_BUILDDIR)/linkcheck"

.PHONY: docs-clean
docs-clean:
	@rm -rf "$(DOCS_BUILDDIR)"

# ----- Nox -------------------------------------------------------------------

.PHONY: nox-list
nox-list:
	@$(PYTHON) -m nox -l

.PHONY: nox
nox:
	@$(PYTHON) -m nox $(NOXOPTS) -s $(NOX_SESSIONS)

# Fix Neo4j (latest), vary nothing else: current interpreter vs latest Neo4j
.PHONY: nox-py
nox-py:
	@$(PYTHON) -m nox $(NOXOPTS) -s py

# Multiple Python versions vs latest Neo4j
.PHONY: nox-pyall
nox-pyall:
	@$(PYTHON) -m nox $(NOXOPTS) -s pyall

# Fix Python (current), vary Neo4j (LTS + latest)
.PHONY: nox-neo4j
nox-neo4j:
	@$(PYTHON) -m nox $(NOXOPTS) -s neo4j

# ----- Cleaning --------------------------------------------------------------

.PHONY: clean
clean:
	@rm -rf \
		".pytest_cache" \
		".nox" \
		"$(DOCS_BUILDDIR)" \
		"dist" \
		"build" \
		"*.egg-info" \
		".coverage" \
		"coverage.xml" \
		"htmlcov"
