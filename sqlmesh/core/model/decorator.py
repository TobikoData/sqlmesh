from __future__ import annotations

import typing as t
from pathlib import Path
import inspect
import re

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlmesh.core.macros import MacroRegistry
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.core import constants as c
from sqlmesh.core.dialect import MacroFunc, parse_one
from sqlmesh.core.model.definition import (
    Model,
    create_python_model,
    create_sql_model,
    create_models_from_blueprints,
    get_model_name,
    parse_defaults_properties,
    render_meta_fields,
    render_model_defaults,
)
from sqlmesh.core.model.kind import ModelKindName, _ModelKind
from sqlmesh.utils import registry_decorator, DECORATOR_RETURN_TYPE
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import build_env, serialize_env


if t.TYPE_CHECKING:
    from sqlmesh.core.audit import ModelAudit


class model(registry_decorator):
    """Specifies a function is a python based model."""

    registry_name = "python_models"
    _dialect: DialectType = None

    def __init__(self, name: t.Optional[str] = None, is_sql: bool = False, **kwargs: t.Any) -> None:
        if not is_sql and "columns" not in kwargs:
            raise ConfigError("Python model must define column schema.")

        self.name_provided = bool(name)
        self.name = name or ""
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
                else exp.DataType.build(
                    str(column_type), dialect=self.kwargs.get("dialect", self._dialect)
                )
            )
            for column_name, column_type in self.kwargs.pop("columns", {}).items()
        }

    def __call__(
        self, func: t.Callable[..., DECORATOR_RETURN_TYPE]
    ) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
        if not self.name_provided:
            self.name = get_model_name(Path(inspect.getfile(func)))
        return super().__call__(func)

    def models(
        self,
        get_variables: t.Callable[[t.Optional[str]], t.Dict[str, str]],
        path: Path,
        module_path: Path,
        dialect: t.Optional[str] = None,
        **loader_kwargs: t.Any,
    ) -> t.List[Model]:
        return create_models_from_blueprints(
            gateway=self.kwargs.get("gateway"),
            blueprints=self.kwargs.get("blueprints"),
            get_variables=get_variables,
            loader=self.model,
            path=path,
            module_path=module_path,
            dialect=dialect,
            **loader_kwargs,
        )

    def model(
        self,
        *,
        module_path: Path,
        path: Path,
        defaults: t.Optional[t.Dict[str, t.Any]] = None,
        macros: t.Optional[MacroRegistry] = None,
        jinja_macros: t.Optional[JinjaMacroRegistry] = None,
        audit_definitions: t.Optional[t.Dict[str, ModelAudit]] = None,
        dialect: t.Optional[str] = None,
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
        physical_schema_mapping: t.Optional[t.Dict[re.Pattern, str]] = None,
        project: str = "",
        default_catalog: t.Optional[str] = None,
        variables: t.Optional[t.Dict[str, t.Any]] = None,
        infer_names: t.Optional[bool] = False,
    ) -> Model:
        """Get the model registered by this function."""
        env: t.Dict[str, t.Any] = {}
        entrypoint = self.func.__name__

        if not self.name_provided and not infer_names:
            raise ConfigError("Python model must have a name.")

        kind = self.kwargs.get("kind", None)
        if kind is not None:
            if isinstance(kind, _ModelKind):
                from sqlmesh.core.console import get_console

                get_console().log_warning(
                    f"""Python model "{self.name}"'s `kind` argument was passed a SQLMesh `{type(kind).__name__}` object. This may result in unexpected behavior - provide a dictionary instead."""
                )
            elif isinstance(kind, dict):
                if "name" not in kind or not isinstance(kind.get("name"), ModelKindName):
                    raise ConfigError(
                        f"""Python model "{self.name}"'s `kind` dictionary must contain a `name` key with a valid ModelKindName enum value."""
                    )

        build_env(self.func, env=env, name=entrypoint, path=module_path)

        rendered_fields = render_meta_fields(
            fields={"name": self.name, **self.kwargs},
            module_path=module_path,
            macros=macros,
            jinja_macros=jinja_macros,
            variables=variables,
            path=path,
            dialect=dialect,
            default_catalog=default_catalog,
        )

        rendered_name = rendered_fields["name"]
        if isinstance(rendered_name, exp.Expression):
            rendered_fields["name"] = rendered_name.sql(dialect=dialect)

        rendered_defaults = (
            render_model_defaults(
                defaults=defaults,
                module_path=module_path,
                macros=macros,
                jinja_macros=jinja_macros,
                variables=variables,
                path=path,
                dialect=dialect,
                default_catalog=default_catalog,
            )
            if defaults
            else {}
        )

        rendered_defaults = parse_defaults_properties(rendered_defaults, dialect=dialect)

        common_kwargs = {
            "defaults": rendered_defaults,
            "path": path,
            "time_column_format": time_column_format,
            "python_env": serialize_env(env, path=module_path),
            "physical_schema_mapping": physical_schema_mapping,
            "project": project,
            "default_catalog": default_catalog,
            "variables": variables,
            "dialect": dialect,
            "columns": self.columns if self.columns else None,
            "module_path": module_path,
            "macros": macros,
            "jinja_macros": jinja_macros,
            "audit_definitions": audit_definitions,
            **rendered_fields,
        }

        for key in ("pre_statements", "post_statements", "on_virtual_update"):
            statements = common_kwargs.get(key)
            if statements:
                common_kwargs[key] = [
                    parse_one(s, dialect=common_kwargs.get("dialect")) if isinstance(s, str) else s
                    for s in statements
                ]

        if self.is_sql:
            query = MacroFunc(this=exp.Anonymous(this=entrypoint))
            return create_sql_model(query=query, **common_kwargs)
        return create_python_model(entrypoint=entrypoint, **common_kwargs)
