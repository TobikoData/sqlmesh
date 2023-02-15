from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.utils.conversions import ensure_bool, try_str_to_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

T = t.TypeVar("T", bound="BaseConfig")

DEFAULT_PROJECT_FILE = "dbt_project.yml"


def project_config_path(project_root: t.Optional[Path] = None) -> Path:
    """
    Get the path to the DBT project config file

    Args:
        project_root: Path to the DBT project root. If not specified, the current
                      directory is used.

    Returns:
        Path to the DBT project config file
    """
    project_root = project_root or Path()
    path = Path(project_root, DEFAULT_PROJECT_FILE)
    if not path.exists():
        raise ConfigError(f"Could not find {DEFAULT_PROJECT_FILE} for this project")

    return path


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
    variables: t.Dict[str, bool] = {}

    def union(self, other: Dependencies) -> Dependencies:
        dependencies = Dependencies()
        dependencies.macros = self.macros.union(other.macros)
        dependencies.sources = self.sources.union(other.sources)
        dependencies.refs = self.refs.union(other.refs)
        dependencies.variables = {**self.variables, **other.variables}

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

    def replace(self, other: T) -> None:
        """
        Replace the contents of this instance with the passed in instance.

        Args:
            other: The instance to apply to this instance
        """
        for field in other.__fields_set__:
            setattr(self, field, getattr(other, field))


def parse_meta(v: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    for key, value in v.items():
        if isinstance(value, str):
            v[key] = try_str_to_bool(value)

    return v
