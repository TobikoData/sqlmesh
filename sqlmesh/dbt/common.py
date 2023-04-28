from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import Field, validator
from ruamel.yaml.constructor import DuplicateKeyError
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


T = t.TypeVar("T", bound="GeneralConfig")


PROJECT_FILENAME = "dbt_project.yml"

JINJA_ONLY = {
    "adapter",
    "api",
    "exceptions",
    "flags",
    "load_result",
    "modules",
    "run_query",
    "statement",
    "store_result",
    "target",
}


def load_yaml(source: str | Path) -> t.OrderedDict:
    try:
        return load(source, render_jinja=False)
    except DuplicateKeyError as ex:
        raise ConfigError(f"{source}: {ex}" if isinstance(source, Path) else f"{ex}")


def parse_meta(v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    for key, value in v.items():
        if isinstance(value, str):
            v[key] = try_str_to_bool(value)

    return v


class SqlStr(str):
    pass


class DbtConfig(BaseConfig):
    class Config:
        extra = "allow"
        allow_mutation = True
        validate_assignment = True


class QuotingConfig(DbtConfig):
    database: bool = True
    schema_: bool = Field(True, alias="schema")
    identifier: bool = True


class GeneralConfig(DbtConfig):
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
    tests: t.List[t.Any] = []
    enabled: bool = True
    docs: t.Dict[str, t.Any] = {"show": True}
    persist_docs: t.Dict[str, t.Any] = {}
    tags: t.List[str] = []
    meta: t.Dict[str, t.Any] = {}

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
        def render_value(val: t.Any) -> t.Any:
            if type(val) is not SqlStr and type(val) is str:
                val = context.render(val)
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
            value = getattr(rendered, name)
            if value is not None:
                setattr(rendered, name, render_value(value))

        return rendered
