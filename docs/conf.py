"""Sphinx configuration for the Roost docs site."""

from __future__ import annotations

import os
import sys
from importlib.metadata import version as pkg_version

# Make the source importable for autodoc.
sys.path.insert(0, os.path.abspath("../src"))

project = "Roost"
author = "Ashhad Ahsan"
copyright = "2026, Ashhad Ahsan"

try:
    release = pkg_version("pgroost")
except Exception:  # pragma: no cover
    release = "0.0.0"
version = release

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "linkify",
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]
html_logo = "_static/logo.svg"
html_title = f"Roost {release}"

autodoc_member_order = "bysource"
autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = False
