from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.model.definition import Model, create_python_model
from sqlmesh.utils import registry_decorator
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import build_env, serialize_env


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"

    def __init__(self, name: str, **kwargs: t.Any) -> None:
        if not name:
            raise ConfigError("Python model must have a name.")
        if not "columns" in kwargs:
            raise ConfigError("Python model must define column schema.")

        self.name = name
        self.kwargs = kwargs

        # Make sure that argument values are expressions in order to
        # pass validation in ModelMeta.
        calls = self.kwargs.pop("audits", [])
        self.kwargs["audits"] = [
            (call, {})
            if isinstance(call, str)
            else (
                call[0],
                {arg_key: exp.convert(arg_value) for arg_key, arg_value in call[1].items()},
            )
            for call in calls
        ]

        self.columns = {
            column_name: column_type
            if isinstance(column_type, exp.DataType)
            else exp.DataType.build(str(column_type))
            for column_name, column_type in self.kwargs.pop("columns").items()
        }

    def model(
        self,
        *,
        module_path: Path,
        path: Path,
        defaults: t.Optional[t.Dict[str, t.Any]] = None,
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
        physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    ) -> Model:
        """Get the model registered by this function."""
        env: t.Dict[str, t.Any] = {}
        entrypoint = self.func.__name__

        build_env(
            self.func,
            env=env,
            name=entrypoint,
            path=module_path,
        )

        return create_python_model(
            self.name,
            entrypoint,
            defaults=defaults,
            path=path,
            time_column_format=time_column_format,
            python_env=serialize_env(env, path=module_path),
            columns=self.columns,
            physical_schema_override=physical_schema_override,
            **self.kwargs,
        )
