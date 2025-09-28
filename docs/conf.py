# Configuration file for the Sphinx documentation builder.

import os
import sys

sys.path.insert(0, os.path.abspath('../src'))  # Source code dir relative to this file

# -- Project information -----------------------------------------------------

project = 'neo4j-etl-lib'
author = 'Bert Radke'

# The full version, including alpha/beta/rc tags
from etl_lib import __version__

version = __version__
release = version

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.graphviz',
    'sphinx.ext.intersphinx',
    'sphinx_autodoc_typehints',
]

autosummary_generate = True

# Autodoc / type hints
autoclass_content = "class"
autodoc_inherit_docstrings = True
add_module_names = False
autodoc_default_options = {
    'special-members': '__init__',
}
autodoc_typehints = "description"
typehints_fully_qualified = False
python_use_unqualified_type_names = True

# Napoleon (Google/NumPy docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
    "neo4j": ("https://neo4j.com/docs/api/python-driver/current/", None),
}

# Silence known-unresolvable third-party/private refs
nitpick_ignore = [
    ('py:class', 'neo4j._sync.work.session.Session'),
    ('py:class', 'neo4j._data.Record'),
    ('py:class', 'graphdatascience.graph_data_science.GraphDataScience'),
    ('py:class', 'pydantic.main.BaseModel'),
    ('py:class', 'pathlib._local.Path'),
    ('py:class', 'datetime.datetime'),
    ('py:data', 'typing.Any'),
    ('py:data', 'typing.Callable'),
    ('py:data', 'typing.Tuple'),
]

templates_path = ['_templates']

# -- Options for HTML output -------------------------------------------------

# Readthedocs theme
html_theme = "sphinx_rtd_theme"
html_css_files = ["readthedocs-custom.css"]  # Override some CSS settings

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
