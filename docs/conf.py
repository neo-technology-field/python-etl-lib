# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
# Problems with imports? Could try `export PYTHONPATH=$PYTHONPATH:`pwd`` from root project dir...
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

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',  # Core Sphinx library for auto html doc generation from docstrings
    'sphinx.ext.viewcode',  # Add a link to the Python source code for classes, functions etc.
    'sphinx_autodoc_typehints', # Automatically document param types (less noise in class signature)
    'sphinx.ext.napoleon',
    'sphinx.ext.graphviz'
]

autoclass_content = "class"
html_show_sourcelink = False  # Remove 'view source code' from top of page (for html, not python)
autodoc_inherit_docstrings = True  # If no docstring, inherit from base class
set_type_checking_flag = True  # Enable 'expensive' imports for sphinx_autodoc_typehints
autodoc_typehints = "description" # Sphinx-native method. Not as good as sphinx_autodoc_typehints
add_module_names = False # Remove namespaces from class/method signatures

autodoc_default_options = {
    'special-members': '__init__',
}


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# Exclusions
# To exclude a module, use autodoc_mock_imports. Note this may increase build time, a lot.
# (Also, when installing on readthedocs.org, we omit installing Tensorflow and
# Tensorflow Probability so mock them here instead.)
#autodoc_mock_imports = [
    # 'tensorflow',
    # 'tensorflow_probability',
#]
# To exclude a class, function, method or attribute, use autodoc-skip-member. (Note this can also
# be used in reverse, ie. to re-include a particular member that has been excluded.)
# 'Private' and 'special' members (_ and __) are excluded using the Jinja2 templates; from the main
# doc by the absence of specific autoclass directives (ie. :private-members:), and from summary
# tables by explicit 'if-not' statements. Re-inclusion is effective for the main doc though not for
# the summary tables.
# def autodoc_skip_member_callback(app, what, name, obj, skip, options):
#     # This would exclude the Matern12 class and to_default_float function:
#     exclusions = ('Matern12', 'to_default_float')
#     # This would re-include __call__ methods in main doc, previously excluded by templates:
#     inclusions = ('__call__')
#     if name in exclusions:
#         return True
#     elif name in inclusions:
#         return False
#     else:
#         return skip
# def setup(app):
#     # Entry point to autodoc-skip-member
#     app.connect("autodoc-skip-member", autodoc_skip_member_callback)

# -- Options for HTML output -------------------------------------------------

# Readthedocs theme
html_theme = "sphinx_rtd_theme"
html_css_files = ["readthedocs-custom.css"] # Override some CSS settings

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
