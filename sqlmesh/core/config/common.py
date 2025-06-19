from __future__ import annotations

import typing as t
from enum import Enum
import re

from sqlmesh.utils import classproperty
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import field_validator


class EnvironmentSuffixTarget(str, Enum):
    # Intended to create virtual environments in their own schemas, with names like "<model_schema_name>__<env name>". The view name is untouched.
    # For example, a model named 'sqlmesh_example.full_model' created in an environment called 'dev'
    # would have its virtual layer view created as 'sqlmesh_example__dev.full_model'
    SCHEMA = "schema"

    # Intended to create virtual environments in the same schema as their production counterparts by adjusting the table name.
    # For example, a model named 'sqlmesh_example.full_model' created in an environment called 'dev'
    # would have its virtual layer view created as "sqlmesh_example.full_model__dev"
    TABLE = "table"

    # Intended to create virtual environments in their own catalogs to preserve the schema and view name of the models
    # For example, a model named 'sqlmesh_example.full_model' created in an environment called 'dev'
    # with a default catalog of "warehouse" would have its virtual layer view created as "warehouse__dev.sqlmesh_example.full_model"
    # note: this only works for engines that can query across catalogs
    CATALOG = "catalog"

    @property
    def is_schema(self) -> bool:
        return self == EnvironmentSuffixTarget.SCHEMA

    @property
    def is_table(self) -> bool:
        return self == EnvironmentSuffixTarget.TABLE

    @property
    def is_catalog(self) -> bool:
        return self == EnvironmentSuffixTarget.CATALOG

    @classproperty
    def default(cls) -> EnvironmentSuffixTarget:
        return EnvironmentSuffixTarget.SCHEMA

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)


def _concurrent_tasks_validator(v: t.Any) -> int:
    if isinstance(v, str):
        v = int(v)
    if not isinstance(v, int) or v <= 0:
        raise ConfigError(
            f"The number of concurrent tasks must be an integer value greater than 0. '{v}' was provided"
        )
    return v


concurrent_tasks_validator = field_validator(
    "backfill_concurrent_tasks",
    "ddl_concurrent_tasks",
    "concurrent_tasks",
    mode="before",
    check_fields=False,
)(_concurrent_tasks_validator)


def _http_headers_validator(v: t.Any) -> t.Any:
    if isinstance(v, dict):
        return [(key, value) for key, value in v.items()]
    return v


http_headers_validator = field_validator(
    "http_headers",
    mode="before",
    check_fields=False,
)(_http_headers_validator)


def _variables_validator(value: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"Variables must be a dictionary, not {type(value)}")

    def _validate_type(v: t.Any) -> None:
        if isinstance(v, list):
            for item in v:
                _validate_type(item)
        elif isinstance(v, dict):
            for item in v.values():
                _validate_type(item)
        elif v is not None and not isinstance(v, (str, int, float, bool)):
            raise ConfigError(f"Unsupported variable value type: {type(v)}")

    _validate_type(value)
    return {k.lower(): v for k, v in value.items()}


variables_validator = field_validator(
    "variables",
    mode="before",
    check_fields=False,
)(_variables_validator)


def compile_regex_mapping(value: t.Dict[str | re.Pattern, t.Any]) -> t.Dict[re.Pattern, t.Any]:
    """
    Utility function to compile a dict of { "string regex pattern" : "string value" } into { "<re.Pattern>": "string value" }
    """
    compiled_regexes = {}
    for k, v in value.items():
        try:
            compiled_regexes[re.compile(k)] = v
        except re.error:
            raise ConfigError(f"`{k}` is not a valid regular expression.")
    return compiled_regexes
