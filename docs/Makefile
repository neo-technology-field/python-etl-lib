# Minimal makefile for Sphinx documentation

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

.PHONY: help livehtml  # add livehtml here so make never confuses it with a file
livehtml:
	sphinx-autobuild --ignore "$(BUILDDIR)/*" --watch ../src -- "$(SOURCEDIR)" "$(BUILDDIR)/html"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).

# Note: Jupytext converts .py to .ipynb (Sphinx seems to execute Notebook..?)
%: Makefile
	rm -rf _build
	rm -rf source
	sphinx-apidoc -e --no-toc -o ./source ../src/etl_lib ../src/etl_lib/test_utils/** ../src/etl_lib/**/tests/** ../src/etl_lib/**/test_*.py
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
