from __future__ import annotations

import typing as t
from dataclasses import dataclass, field, replace
from pathlib import Path

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.core.model.definition import BUILTIN_METHODS as SQLMESH_PYTHON_BUILTIN
from sqlmesh.dbt.builtin import (
    BUILTIN_JINJA,
    generate_adapter,
    generate_ref,
    generate_source,
    generate_var,
    log,
)
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import render_jinja
from sqlmesh.utils.metaprogramming import Executable, build_env, serialize_env
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load

T = t.TypeVar("T", bound="GeneralConfig")


PROJECT_FILENAME = "dbt_project.yml"


def load_yaml(source: str | Path) -> t.OrderedDict:
    return load(source, render_jinja=False)


@dataclass
class DbtContext:
    """Context for DBT environment"""

    project_root: Path = Path()
    target_name: t.Optional[str] = None
    project_name: t.Optional[str] = None
    project_schema: t.Optional[str] = None
    macros: t.Dict[str, Executable] = field(default_factory=dict)
    _builtins: t.Dict[str, t.Any] = field(default_factory=dict)
    _variables: t.Dict[str, t.Any] = field(default_factory=dict)
    _target: t.Optional[TargetConfig] = None
    _sources: t.Dict[str, str] = field(default_factory=dict)
    _refs: t.Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self._builtins:
            self._builtins = BUILTIN_JINJA.copy()

        self.sources = self._sources
        self.refs = self._refs
        self.variables = self._variables
        if self._target:
            self.target = self._target

    @property
    def target(self) -> TargetConfig:
        if not self._target:
            raise ConfigError(f"Target not set for {self.project_name}")
        return self._target

    @target.setter
    def target(self, value: TargetConfig) -> None:
        self._target = value
        if not self.project_name:
            raise ConfigError(f"Must assign project_name before assigning target")
        self._builtins["target"] = self._target.target_jinja(self.project_name)
        self._builtins["adapter"] = generate_adapter(self._target)

    @property
    def variables(self) -> t.Dict[str, t.Any]:
        return self._variables

    @variables.setter
    def variables(self, value: t.Dict[str, t.Any]) -> None:
        self._variables = value
        self._builtins["var"] = generate_var(self._variables)

    @property
    def refs(self) -> t.Dict[str, str]:
        return self._refs

    @refs.setter
    def refs(self, value: t.Dict[str, str]) -> None:
        self._refs = value
        self._builtins["ref"] = generate_ref(self._refs)

    @property
    def sources(self) -> t.Dict[str, str]:
        return self._sources

    @sources.setter
    def sources(self, value: t.Dict[str, str]) -> None:
        self._sources = value
        self._builtins["source"] = generate_source(self._sources)

    @property
    def builtin_jinja(self) -> t.Dict[str, t.Any]:
        return self._builtins

    @property
    def builtin_python_env(self) -> t.Dict[str, t.Any]:
        env: t.Dict[str, t.Any] = {}
        methods = self.builtin_jinja.copy()
        methods["log"] = log
        for name, method in methods.items():
            # temporary until Iaroslav has the jinja templates working
            if name not in ["target", "adapter"]:
                build_env(method, env=env, name=name, path=Path(__file__).parent)

        return {**serialize_env(env, Path(__file__).parent), **SQLMESH_PYTHON_BUILTIN}

    def render(self, source: str) -> str:
        return render_jinja(source, self._builtins)

    def copy(self) -> DbtContext:
        return replace(self)


class SqlStr(str):
    pass


class Dependencies(PydanticModel):
    """
    DBT dependencies for a model, macro, etc.

    Args:
        macros: The names of macros used
        sources: The "source_name.table_name" for source tables used
        refs: The table_name for models used
        variables: The names of variables used, mapped to a flag that indicates whether their
            definition is optional or not.
    """

    macros: t.Set[str] = set()
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


class GeneralConfig(BaseConfig):
    """
    General DBT configuration properties for models, sources, seeds, columns, etc.

    Args:
        description: Description of element
        tests: Tests for the element
        enabled: When false, the element is ignored
        docs: Documentation specific configuration
        perist_docs: Persist resource descriptions as column and/or relation comments in the database
        tags: List of tags that can be used for element grouping
        meta: Dictionary of metadata for the element
    """

    start: t.Optional[str] = None
    description: t.Optional[str] = None
    # TODO add test support
    tests: t.Dict[str, t.Any] = {}
    enabled: bool = True
    docs: t.Dict[str, t.Any] = {"show": True}
    persist_docs: t.Dict[str, t.Any] = {}
    tags: t.List[str] = []
    meta: t.Dict[str, t.Any] = {}

    class Config:
        extra = "allow"
        allow_mutation = True

    @validator("enabled", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("docs", pre=True)
    def _validate_dict(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for key, value in v.items():
            if isinstance(value, str):
                v[key] = try_str_to_bool(value)

        return v

    @validator("persist_docs", pre=True)
    def _validate_persist_docs(cls, v: t.Dict[str, str]) -> t.Dict[str, bool]:
        return {key: bool(value) for key, value in v.items()}

    @validator("tags", pre=True)
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        return parse_meta(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **BaseConfig._FIELD_UPDATE_STRATEGY,
        **{
            "tests": UpdateStrategy.KEY_UPDATE,
            "docs": UpdateStrategy.KEY_UPDATE,
            "persist_docs": UpdateStrategy.KEY_UPDATE,
            "tags": UpdateStrategy.EXTEND,
            "meta": UpdateStrategy.KEY_UPDATE,
        },
    }

    _SQL_FIELDS: t.ClassVar[t.List[str]] = []

    def replace(self, other: T) -> None:
        """
        Replace the contents of this instance with the passed in instance.

        Args:
            other: The instance to apply to this instance
        """
        for field in other.__fields_set__:
            setattr(self, field, getattr(other, field))

    def render_config(self: T, context: DbtContext) -> T:
        methods = context.builtin_jinja

        def render_value(val: t.Any) -> t.Any:
            if type(val) is not SqlStr and type(val) is str:
                val = render_jinja(val, methods)
            elif isinstance(val, GeneralConfig):
                for name in val.__fields__:
                    setattr(val, name, render_value(getattr(val, name)))
            elif isinstance(val, list):
                for i in range(len(val)):
                    val[i] = render_value(val[i])
            elif isinstance(val, set):
                for set_val in val:
                    val.remove(set_val)
                    val.add(render_value(set_val))
            elif isinstance(val, dict):
                for k in val:
                    val[k] = render_value(val[k])

            return val

        rendered = self.copy(deep=True)
        for name in rendered.__fields__:
            setattr(rendered, name, render_value(getattr(rendered, name)))

        return rendered


def parse_meta(v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    for key, value in v.items():
        if isinstance(value, str):
            v[key] = try_str_to_bool(value)

    return v
