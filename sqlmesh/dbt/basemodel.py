from __future__ import annotations

import typing as t
from abc import abstractmethod
from enum import Enum
from pathlib import Path

from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import RelationType
from jinja2 import nodes
from jinja2.exceptions import UndefinedError
from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.model import Model
from sqlmesh.dbt.adapter import ParsetimeAdapter
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
    yaml_to_columns,
)
from sqlmesh.dbt.common import DbtConfig, GeneralConfig, QuotingConfig, SqlStr
from sqlmesh.dbt.context import DbtContext
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.date import date_dict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroReference, extract_macro_references
from sqlmesh.utils.pydantic import PydanticModel

BMC = t.TypeVar("BMC", bound="BaseModelConfig")


class Dependencies(PydanticModel):
    """
    DBT dependencies for a model, macro, etc.

    Args:
        macros: The references to macros
        sources: The "source_name.table_name" for source tables used
        refs: The table_name for models used
        variables: The names of variables used, mapped to a flag that indicates whether their
            definition is optional or not.
    """

    macros: t.Set[MacroReference] = set()
    sources: t.Set[str] = set()
    refs: t.Set[str] = set()
    variables: t.Set[str] = set()

    def union(self, other: Dependencies) -> Dependencies:
        dependencies = Dependencies()
        dependencies.macros = self.macros | other.macros
        dependencies.sources = self.sources | other.sources
        dependencies.refs = self.refs | other.refs
        dependencies.variables = self.variables | other.variables

        return dependencies

    def dict(self, *args: t.Any, **kwargs: t.Any) -> t.Dict[str, t.Any]:
        # See https://github.com/pydantic/pydantic/issues/1090
        exclude = kwargs.pop("exclude", None) or set()

        out = super().dict(*args, **kwargs, exclude={*exclude, "macros"})
        if "macros" not in exclude:
            out["macros"] = [macro.dict() for macro in self.macros]

        return out


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


class Hook(DbtConfig):
    """
    Args:
        sql: The sql to execute.
        transaction: bool indicating if the hook is executed in the same transaction as the model query.
    """

    sql: SqlStr
    transaction: bool = True  # TODO not yet supported


class BaseModelConfig(GeneralConfig):
    """
    Args:
        owner: The owner of the model.
        stamp: An optional arbitrary string sequence used to create new model versions without making
            changes to any of the functional components of the definition.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        path: The file path of the model
        target_schema: The schema for the profile target
        dependencies: The macro, source, var, and ref dependencies used to execute the model and its hooks
        database: Database the model is stored in
        schema: Custom schema name added to the model schema name
        alias: Relation identifier for this model instead of the filename
        pre-hook: List of SQL statements to run before the model is built
        post-hook: List of SQL statements to run after the model is built
        full_refresh: Forces the model to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this model
        columns: Column information for the model
        quoting: Define which components of the qualified name (database, schema, identifier) to quote when resolving the ref() method
    """

    # sqlmesh fields
    owner: t.Optional[str] = None
    stamp: t.Optional[str] = None
    storage_format: t.Optional[str] = None
    path: Path = Path()
    target_schema: str = ""
    dependencies: Dependencies = Dependencies()

    # DBT configuration fields
    database: t.Optional[str] = None
    schema_: t.Optional[str] = Field(None, alias="schema")
    alias: t.Optional[str] = None
    pre_hook: t.List[Hook] = Field([], alias="pre-hook")
    post_hook: t.List[Hook] = Field([], alias="post-hook")
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    columns: t.Dict[str, ColumnConfig] = {}
    quoting: QuotingConfig = Field(default_factory=QuotingConfig)

    @validator("pre_hook", "post_hook", pre=True)
    def _validate_hooks(cls, v: t.Union[str, t.List[t.Union[SqlStr, str]]]) -> t.List[Hook]:
        hooks = []
        for hook in ensure_list(v):
            if isinstance(hook, Hook):
                hooks.append(hook)
            elif isinstance(hook, str):
                hooks.append(Hook(sql=hook))
            elif isinstance(hook, dict):
                hooks.append(Hook(**hook))
            else:
                raise ConfigError(f"Invalid hook data: {hook}")

        return hooks

    @validator("full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("grants", pre=True)
    def _validate_grants(cls, v: t.Dict[str, str]) -> t.Dict[str, t.List[str]]:
        return {key: ensure_list(value) for key, value in v.items()}

    @validator("columns", pre=True)
    def _validate_columns(cls, v: t.Any) -> t.Dict[str, ColumnConfig]:
        if isinstance(v, dict) and all(isinstance(col, ColumnConfig) for col in v.values()):
            return v

        return yaml_to_columns(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{
            "grants": UpdateStrategy.KEY_EXTEND,
            "path": UpdateStrategy.IMMUTABLE,
            "pre-hook": UpdateStrategy.EXTEND,
            "post-hook": UpdateStrategy.EXTEND,
            "columns": UpdateStrategy.KEY_EXTEND,
        },
    }

    @property
    def all_sql(self) -> SqlStr:
        return SqlStr(
            "\n".join(
                [hook.sql for hook in self.pre_hook]
                + [self.sql_no_config]
                + [hook.sql for hook in self.post_hook]
            )
        )

    @property
    def sql_no_config(self) -> SqlStr:
        return SqlStr("")

    @property
    def sql_embedded_config(self) -> SqlStr:
        return SqlStr("")

    @property
    def table_schema(self) -> str:
        """
        Get the full schema name
        """
        return "_".join(part for part in (self.target_schema, self.schema_) if part)

    @property
    def table_name(self) -> str:
        """
        Get the table name
        """
        return self.alias or self.path.stem

    @property
    def model_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        return ".".join(
            part for part in (self.database, self.table_schema, self.table_name) if part
        )

    @property
    def model_materialization(self) -> Materialization:
        return Materialization.TABLE

    @property
    def model_dialect(self) -> t.Optional[str]:
        return None

    @property
    def relation_info(self) -> AttributeDict[str, t.Any]:
        if self.model_materialization == Materialization.VIEW:
            relation_type = RelationType.View
        elif self.model_materialization == Materialization.EPHEMERAL:
            relation_type = RelationType.CTE
        else:
            relation_type = RelationType.Table

        return AttributeDict(
            {
                "database": self.database,
                "schema": self.table_schema,
                "identifier": self.table_name,
                "type": relation_type.value,
                "quote_policy": AttributeDict(self.quoting.dict()),
            }
        )

    def attribute_dict(self) -> AttributeDict[str, t.Any]:
        return AttributeDict(self.dict())

    def model_function(self) -> AttributeDict[str, t.Any]:
        return AttributeDict({"config": self.attribute_dict()})

    def sqlmesh_model_kwargs(self, model_context: DbtContext) -> t.Dict[str, t.Any]:
        """Get common sqlmesh model parameters"""
        jinja_macros = model_context.jinja_macros.trim(self.dependencies.macros)
        jinja_macros.global_objs.update(
            {
                "this": self.relation_info,
                "model": self.model_function(),
                "schema": self.table_schema,
                "config": self.attribute_dict(),
                **model_context.jinja_globals,  # type: ignore
            }
        )

        optional_kwargs: t.Dict[str, t.Any] = {}
        for field in ("description", "owner", "stamp", "storage_format"):
            field_val = getattr(self, field, None) or self.meta.get(field, None)
            if field_val:
                optional_kwargs[field] = field_val

        return {
            "columns": column_types_to_sqlmesh(self.columns) or None,
            "column_descriptions_": column_descriptions_to_sqlmesh(self.columns) or None,
            "depends_on": {model_context.refs[ref] for ref in self.dependencies.refs},
            "jinja_macros": jinja_macros,
            "path": self.path,
            "pre": [
                exp
                for hook in self.pre_hook
                for exp in d.parse(
                    hook.sql, default_dialect=self.model_dialect or model_context.dialect
                )
            ],
            "post": [
                exp
                for hook in self.post_hook
                for exp in d.parse(
                    hook.sql, default_dialect=self.model_dialect or model_context.dialect
                )
            ],
            **optional_kwargs,
        }

    def render_config(self: BMC, context: DbtContext) -> BMC:
        rendered = super().render_config(context)
        rendered = ModelSqlRenderer(context, rendered).enriched_config

        rendered_dependencies = rendered.dependencies
        for dependency in rendered_dependencies.refs:
            model = context.models.get(dependency)
            if model and model.materialized == Materialization.EPHEMERAL:
                rendered.dependencies = rendered.dependencies.union(
                    model.render_config(context).dependencies
                )
                rendered.dependencies.refs.discard(dependency)

        return rendered

    @abstractmethod
    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Convert DBT model into sqlmesh Model"""

    def _context_for_dependencies(
        self, context: DbtContext, dependencies: Dependencies
    ) -> DbtContext:
        model_context = context.copy()

        models = {}
        seeds = {}
        sources = {}

        for ref in self.dependencies.refs:
            if ref in context.seeds:
                seeds[ref] = context.seeds[ref]
            elif ref in context.models:
                models[ref] = context.models[ref]
            else:
                raise ConfigError(f"Model '{ref}' was not found for model '{self.table_name}'.")

        for source in self.dependencies.sources:
            if source in context.sources:
                sources[source] = context.sources[source]
            else:
                raise ConfigError(f"Source '{source}' was not found for model '{self.table_name}'.")

        model_context.sources = sources
        model_context.seeds = seeds
        model_context.models = models
        model_context.variables = {
            name: value
            for name, value in context.variables.items()
            if name in dependencies.variables
        }

        return model_context


class ModelSqlRenderer(t.Generic[BMC]):
    def __init__(self, context: DbtContext, config: BMC):
        from sqlmesh.dbt.builtin import create_builtin_globals

        self.context = context
        self.config = config

        self._captured_dependencies: Dependencies = Dependencies()
        self._rendered_sql: t.Optional[str] = None
        self._enriched_config: BMC = config.copy()

        self._jinja_globals = create_builtin_globals(
            jinja_macros=context.jinja_macros,
            jinja_globals={
                **context.jinja_globals,
                **date_dict(c.EPOCH, c.EPOCH, c.EPOCH),
                "config": lambda *args, **kwargs: "",
                "ref": self._ref,
                "var": self._var,
                "source": self._source,
                "this": self.config.relation_info,
                "model": self.config.model_function(),
                "schema": self.config.table_schema,
            },
            engine_adapter=None,
        )

        # Set the adapter separately since it requires jinja globals to passed into it.
        self._jinja_globals["adapter"] = ModelSqlRenderer.TrackingAdapter(
            self,
            context.jinja_macros,
            jinja_globals=self._jinja_globals,
            dialect=context.engine_adapter.dialect if context.engine_adapter else "",
        )

        self.jinja_env = self.context.jinja_macros.build_environment(**self._jinja_globals)

    @property
    def enriched_config(self) -> BMC:
        if self._rendered_sql is None:
            self._enriched_config = self._update_with_sql_config(self._enriched_config)
            self._enriched_config.dependencies = Dependencies(
                macros=extract_macro_references(self._enriched_config.all_sql)
            )
            self.render()
            self._enriched_config.dependencies = self._enriched_config.dependencies.union(
                self._captured_dependencies
            )
        return self._enriched_config

    def render(self) -> str:
        if self._rendered_sql is None:
            try:
                self._rendered_sql = self.jinja_env.from_string(
                    self._enriched_config.all_sql
                ).render()
            except UndefinedError as e:
                raise ConfigError(e.message)
        return self._rendered_sql

    def _update_with_sql_config(self, config: BMC) -> BMC:
        def _extract_value(node: t.Any) -> t.Any:
            if not isinstance(node, nodes.Node):
                return node
            if isinstance(node, nodes.Const):
                return _extract_value(node.value)
            if isinstance(node, nodes.TemplateData):
                return _extract_value(node.data)
            if isinstance(node, nodes.List):
                return [_extract_value(val) for val in node.items]
            if isinstance(node, nodes.Dict):
                return {_extract_value(pair.key): _extract_value(pair.value) for pair in node.items}
            if isinstance(node, nodes.Tuple):
                return tuple(_extract_value(val) for val in node.items)

            return self.jinja_env.from_string(nodes.Template([nodes.Output([node])])).render()

        for call in self.jinja_env.parse(self._enriched_config.sql_embedded_config).find_all(
            nodes.Call
        ):
            if not isinstance(call.node, nodes.Name) or call.node.name != "config":
                continue
            config = config.update_with(
                {kwarg.key: _extract_value(kwarg.value) for kwarg in call.kwargs}
            )

        return config

    def _ref(self, package_name: str, model_name: t.Optional[str] = None) -> BaseRelation:
        self._captured_dependencies.refs.add(package_name)
        return BaseRelation.create()

    def _var(self, name: str, default: t.Optional[str] = None) -> t.Any:
        if default is None and name not in self.context.variables:
            raise ConfigError(
                f"Variable '{name}' was not found for model '{self.config.table_name}'."
            )
        self._captured_dependencies.variables.add(name)
        return self.context.variables.get(name, default)

    def _source(self, source_name: str, table_name: str) -> BaseRelation:
        full_name = ".".join([source_name, table_name])
        self._captured_dependencies.sources.add(full_name)
        return BaseRelation.create()

    class TrackingAdapter(ParsetimeAdapter):
        def __init__(self, outer_self: ModelSqlRenderer, *args: t.Any, **kwargs: t.Any):
            super().__init__(*args, **kwargs)
            self.outer_self = outer_self
            self.context = outer_self.context

        def dispatch(self, name: str, package: t.Optional[str] = None) -> t.Callable:
            macros = (
                self.context.jinja_macros.packages.get(package, {})
                if package is not None
                else self.context.jinja_macros.root_macros
            )
            for target_name in macros:
                if target_name.endswith(f"__{name}"):
                    self.outer_self._captured_dependencies.macros.add(
                        MacroReference(package=package, name=target_name)
                    )
            return super().dispatch(name, package=package)
