from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core import constants as c
from sqlmesh.core.model.definition import Model, create_python_model
from sqlmesh.utils import registry_decorator
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import build_env, serialize_env


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.kwargs = kwargs

        if not self.name:
            raise ConfigError("Python model must have a name.")
        if not "columns" in self.kwargs:
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
        entrypoint = self.func.__name__

        build_env(
            self.func,
            env=env,
            name=entrypoint,
            path=module_path,
        )

        return create_python_model(
            entrypoint,
            path=path,
            time_column_format=time_column_format,
            python_env=serialize_env(env, path=module_path),
            name=self.name,
            **self.kwargs,
        )
