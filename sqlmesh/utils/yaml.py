from __future__ import annotations

import io
import typing as t
from collections import OrderedDict
from os import getenv
from pathlib import Path

from jinja2 import Environment
from ruamel.yaml import YAML, CommentedMap

from sqlmesh.utils.errors import SQLMeshError

yaml = YAML()
yaml_env = Environment()

yaml_env.globals.update(env_var=lambda key, default=None: getenv(key, default))


def load(source: str | Path, raise_if_empty: bool = True) -> t.OrderedDict:
    """Loads a YAML object from either a raw string or a file."""
    if isinstance(source, Path):
        with open(source, "r", encoding="utf-8") as file:
            source = file.read()

    contents = yaml.load(yaml_env.from_string(source).render())
    if contents is None:
        if raise_if_empty:
            raise SQLMeshError(f"YAML source '{source}' can't be empty.")
        return OrderedDict()

    return contents


def dumps(value: CommentedMap | OrderedDict) -> str:
    """Dumps a ruamel.yaml loaded object and converts it into a string"""
    result = io.StringIO()
    yaml.dump(value, result)
    return result.getvalue()
