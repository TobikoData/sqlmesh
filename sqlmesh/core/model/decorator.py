from __future__ import annotations

import typing as t
from pathlib import Path

from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import MacroFunc
from sqlmesh.core.model.definition import Model, create_python_model, create_sql_model
from sqlmesh.utils import registry_decorator
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import build_env, serialize_env


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"

    def __init__(self, name: str, is_sql: bool = False, **kwargs: t.Any) -> None:
        if not name:
            raise ConfigError("Python model must have a name.")

        if not is_sql and "columns" not in kwargs:
            raise ConfigError("Python model must define column schema.")

        self.name = name
        self.is_sql = is_sql
        self.kwargs = kwargs

        # Make sure that argument values are expressions in order to pass validation in ModelMeta.
        calls = self.kwargs.pop("audits", [])
        self.kwargs["audits"] = [
            (
                (call, {})
                if isinstance(call, str)
                else (
                    call[0],
                    {
                        arg_key: exp.convert(
                            tuple(arg_value) if isinstance(arg_value, list) else arg_value
                        )
                        for arg_key, arg_value in call[1].items()
                    },
                )
            )
            for call in calls
        ]

        if "default_catalog" in kwargs:
            raise ConfigError("`default_catalog` cannot be set on a per-model basis.")

        self.columns = {
            column_name: (
                column_type
                if isinstance(column_type, exp.DataType)
                else exp.DataType.build(str(column_type))
            )
            for column_name, column_type in self.kwargs.pop("columns", {}).items()
        }

    def model(
        self,
        *,
        module_path: Path,
        path: Path,
        defaults: t.Optional[t.Dict[str, t.Any]] = None,
        dialect: t.Optional[str] = None,
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
        physical_schema_override: t.Optional[t.Dict[str, str]] = None,
        project: str = "",
        default_catalog: t.Optional[str] = None,
    ) -> Model:
        """Get the model registered by this function."""
        env: t.Dict[str, t.Any] = {}
        entrypoint = self.func.__name__

        build_env(self.func, env=env, name=entrypoint, path=module_path)

        common_kwargs = dict(
            defaults=defaults,
            path=path,
            time_column_format=time_column_format,
            python_env=serialize_env(env, path=module_path),
            physical_schema_override=physical_schema_override,
            project=project,
            default_catalog=default_catalog,
            **self.kwargs,
        )

        dialect = common_kwargs.pop("dialect", dialect)
        for key in ("pre_statements", "post_statements"):
            statements = common_kwargs.get(key)
            if statements:
                common_kwargs[key] = [exp.maybe_parse(s, dialect=dialect) for s in statements]

        if self.is_sql:
            query = MacroFunc(this=exp.Anonymous(this=entrypoint))
            return create_sql_model(
                self.name, query, module_path=module_path, dialect=dialect, **common_kwargs
            )

        return create_python_model(
            self.name, entrypoint, columns=self.columns, dialect=dialect, **common_kwargs
        )
