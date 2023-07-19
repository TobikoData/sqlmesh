from __future__ import annotations

import typing as t

from pydantic import validator
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core.dialect import parse
from sqlmesh.utils import str_to_bool
from sqlmesh.utils.errors import ConfigError


def parse_expression(
    v: t.Union[t.List[str], t.List[exp.Expression], str, exp.Expression, t.Callable, None],
    values: t.Dict[str, t.Any],
) -> t.List[exp.Expression] | exp.Expression | t.Callable | None:
    """Helper method to deserialize SQLGlot expressions in Pydantic Models."""
    if v is None:
        return None

    if callable(v):
        return v

    dialect = values.get("dialect")

    if isinstance(v, list):
        return [
            e
            for expressions in (
                parse(i, default_dialect=dialect) if not isinstance(i, exp.Expression) else [i]
                for i in v
            )
            for e in expressions
        ]

    if isinstance(v, str):
        return seq_get(parse(v, default_dialect=dialect), 0)

    if not v:
        raise ConfigError(f"Could not parse {v}")

    return v


expression_validator = validator(
    "query",
    "expressions_",
    "pre_statements_",
    "post_statements_",
    pre=True,
    allow_reuse=True,
    check_fields=False,
)(parse_expression)


def parse_bool(v: t.Any) -> bool:
    if isinstance(v, exp.Boolean):
        return v.this
    if isinstance(v, exp.Expression):
        return str_to_bool(v.name)
    return str_to_bool(str(v or ""))


bool_validator = validator(
    "skip",
    "blocking",
    "forward_only",
    "disable_restatement",
    "insert_overwrite",
    pre=True,
    allow_reuse=True,
    check_fields=False,
)(parse_bool)
