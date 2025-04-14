from __future__ import annotations

import io
import typing as t
from decimal import Decimal
from os import getenv
from pathlib import Path

from ruamel import yaml

from sqlmesh.core.constants import VAR
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.jinja import ENVIRONMENT, create_var

JINJA_METHODS = {
    "env_var": lambda key, default=None: getenv(key, default),
}


def YAML(typ: t.Optional[str] = "safe") -> yaml.YAML:
    yaml_obj = yaml.YAML(typ=typ)

    # Ruamel doesn't know how to serialize Decimal values. This is problematic when,
    # e.g., we're trying to auto-generate a unit test whose body contains Decimal data.
    # This is a best-effort approach to solve this by serializing them as strings.
    yaml_obj.representer.add_representer(
        Decimal, lambda dumper, data: dumper.represent_str(str(data))
    )

    return yaml_obj


def load(
    source: str | Path,
    raise_if_empty: bool = True,
    render_jinja: bool = True,
    allow_duplicate_keys: bool = False,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
) -> t.Dict:
    """Loads a YAML object from either a raw string or a file."""
    path: t.Optional[Path] = None

    if isinstance(source, Path):
        path = source
        with open(source, "r", encoding="utf-8") as file:
            source = file.read()

    if render_jinja:
        source = ENVIRONMENT.from_string(source).render(
            {
                **JINJA_METHODS,
                VAR: create_var(variables or {}),
            }
        )

    yaml = YAML()
    yaml.allow_duplicate_keys = allow_duplicate_keys
    contents = yaml.load(source)
    if contents is None:
        if raise_if_empty:
            error_path = f" '{path}'" if path else ""
            raise SQLMeshError(f"YAML source{error_path} can't be empty.")
        return {}

    return contents


@t.overload
def dump(value: t.Any, stream: io.IOBase) -> None: ...


@t.overload
def dump(value: t.Any) -> str: ...


def dump(value: t.Any, stream: t.Optional[io.IOBase] = None) -> t.Optional[str]:
    """Dumps a ruamel.yaml loaded object and converts it into a string or writes it to a stream."""
    result = io.StringIO()
    YAML(typ=None).dump(value, stream or result)
    return None if stream else result.getvalue()
