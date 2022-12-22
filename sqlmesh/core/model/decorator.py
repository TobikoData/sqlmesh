from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.model.definition import Model
from sqlmesh.utils import registry_decorator
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import build_env, serialize_env


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"

    def __init__(self, definition: str = "", **kwargs):
        self.kwargs = kwargs
        self.expressions = d.parse_model(
            definition, default_dialect=self.kwargs.get("dialect")
        )

        if not self.expressions:
            self.expressions.insert(
                0,
                d.Model(
                    expressions=[
                        exp.Property(this="name", value=self.kwargs.pop("name", None))
                    ]
                ),
            )

        self.name = ""
        columns = "columns" in self.kwargs

        for prop in self.expressions[0].expressions:
            prop_name = prop.name.lower()
            if prop_name == "name":
                self.name = prop.text("value")
            elif prop_name == "columns":
                columns = True

        if not self.name:
            raise ConfigError("Python model must have a name.")
        if not columns:
            raise ConfigError("Python model must define column schema.")

    def model(
        self,
        *,
        module_path: Path,
        path: Path,
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    ) -> Model:
        """Get the model registered by this function."""
        env: t.Dict[str, t.Any] = {}
        name = self.func.__name__

        build_env(
            self.func,
            env=env,
            name=name,
            path=module_path,
        )

        expressions = [
            *self.expressions,
            d.MacroVar(this=name),
        ]

        return Model.load(
            expressions,
            path=path,
            time_column_format=time_column_format,
            python_env=serialize_env(env, path=module_path),
            **self.kwargs,
        )
