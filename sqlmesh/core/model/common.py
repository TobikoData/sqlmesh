from __future__ import annotations

import typing as t

from pydantic import validator
from sqlglot import exp, maybe_parse
from sqlglot.expressions import split_num_words
from sqlglot.helper import seq_get

from sqlmesh.core.dialect import parse
from sqlmesh.utils.errors import ConfigError


def parse_model_name(name: str) -> t.Tuple[t.Optional[str], t.Optional[str], str]:
    """Convert a model name into table parts.

    Args:
        name: model name.

    Returns:
        A tuple consisting of catalog, schema, table name.
    """
    return split_num_words(name, ".", 3)  # type: ignore


def parse_expression(
    v: t.Union[t.List[str], t.List[exp.Expression], str, exp.Expression, t.Callable, None],
) -> t.List[exp.Expression] | exp.Expression | t.Callable | None:
    """Helper method to deserialize SQLGlot expressions in Pydantic Models."""
    if v is None:
        return None

    if callable(v):
        return v

    if isinstance(v, list):
        return [e for e in (maybe_parse(i) for i in v) if e]

    if isinstance(v, str):
        return seq_get(parse(v), 0)

    if not v:
        raise ConfigError(f"Could not parse {v}")

    return v


expression_validator = validator(
    "query", "expressions_", pre=True, allow_reuse=True, check_fields=False
)(parse_expression)
