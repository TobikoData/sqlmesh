from __future__ import annotations

import re
import typing as t
from pathlib import Path

from pydantic import Field, validator
from ruamel.yaml.constructor import DuplicateKeyError
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroReference
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load

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


def load_yaml(source: str | Path) -> t.Dict:
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
    database: t.Optional[bool] = None
    schema_: t.Optional[bool] = Field(default=None, alias="schema")
    identifier: t.Optional[bool] = None

    @validator("database", "schema_", "identifier", pre=True)
    def _validate_bool(cls, v: t.Optional[bool]) -> t.Optional[bool]:
        if v is None:
            return None
        return ensure_bool(v)


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

    @property
    def attribute_dict(self) -> AttributeDict[str, t.Any]:
        return AttributeDict(self.dict())

    def replace(self, other: T) -> None:
        """
        Replace the contents of this instance with the passed in instance.

        Args:
            other: The instance to apply to this instance
        """
        for field in other.__fields_set__:
            setattr(self, field, getattr(other, field))


class Dependencies(PydanticModel):
    """
    DBT dependencies for a model, macro, etc.

    Args:
        macros: The references to macros
        sources: The "source_name.table_name" for source tables used
        refs: The table_name for models used
    """

    macros: t.Set[MacroReference] = set()
    sources: t.Set[str] = set()
    refs: t.Set[str] = set()

    def union(self, other: Dependencies) -> Dependencies:
        return Dependencies(
            macros=self.macros | other.macros,
            sources=self.sources | other.sources,
            refs=self.refs | other.refs,
        )

    def dict(self, *args: t.Any, **kwargs: t.Any) -> t.Dict[str, t.Any]:
        # See https://github.com/pydantic/pydantic/issues/1090
        exclude = kwargs.pop("exclude", None) or set()

        out = super().dict(*args, **kwargs, exclude={*exclude, "macros"})
        if "macros" not in exclude:
            out["macros"] = [macro.dict() for macro in self.macros]

        return out


def extract_jinja_config(input: str) -> t.Tuple[str, str]:
    def jinja_end(sql: str, start: int) -> int:
        cursor = start
        quote = None
        while cursor < len(sql):
            if sql[cursor] in ('"', "'"):
                if quote is None:
                    quote = sql[cursor]
                elif quote == sql[cursor]:
                    quote = None
            if sql[cursor : cursor + 2] == "}}" and quote is None:
                return cursor + 2
            cursor += 1
        return cursor

    no_config = input
    only_config = ""
    matches = re.findall(r"{{\s*config\s*\(", no_config)
    for match in matches:
        start = no_config.find(match)
        if start == -1:
            continue
        extracted = no_config[start : jinja_end(no_config, start)]
        only_config = SqlStr("\n".join([only_config, extracted]) if only_config else extracted)
        no_config = SqlStr(no_config.replace(extracted, "").strip())

    return (no_config, only_config)
