from __future__ import annotations

import io
import typing as t
from collections import OrderedDict
from os import getenv
from pathlib import Path

from ruamel.yaml import YAML, CommentedMap

from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.jinja import ENVIRONMENT

yaml = YAML()

JINJA_METHODS = {
    "env_var": lambda key, default=None: getenv(key, default),
}


def load(
    source: str | Path, raise_if_empty: bool = True, render_jinja: t.Optional[bool] = True
) -> t.OrderedDict:
    """Loads a YAML object from either a raw string or a file."""
    path: t.Optional[Path] = None

    if isinstance(source, Path):
        path = source
        with open(source, "r", encoding="utf-8") as file:
            source = file.read()

    if render_jinja:
        source = ENVIRONMENT.from_string(source).render(JINJA_METHODS)

    contents = yaml.load(source)
    if contents is None:
        if raise_if_empty:
            error_path = f" '{path}'" if path else ""
            raise SQLMeshError(f"YAML source{error_path} can't be empty.")
        return OrderedDict()

    return contents


def dumps(value: CommentedMap | OrderedDict) -> str:
    """Dumps a ruamel.yaml loaded object and converts it into a string"""
    result = io.StringIO()
    yaml.dump(value, result)
    return result.getvalue()
