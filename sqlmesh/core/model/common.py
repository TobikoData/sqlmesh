from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.helper import ensure_list, seq_get
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core.dialect import parse, parse_one
from sqlmesh.utils import str_to_bool
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.pydantic import field_validator, field_validator_v1_args


@field_validator_v1_args
def parse_expression(
    cls: t.Type,
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


@field_validator_v1_args
def parse_expressions(cls: t.Type, v: t.Any, values: t.Dict[str, t.Any]) -> t.List[exp.Expression]:
    dialect = values.get("dialect")

    if isinstance(v, (exp.Tuple, exp.Array)):
        expressions: t.List[exp.Expression] = v.expressions
    elif isinstance(v, exp.Expression):
        expressions = [v]
    else:
        expressions = [
            parse_one(entry, dialect=dialect) if isinstance(entry, str) else entry
            for entry in ensure_list(v)
        ]

    results = []

    for expr in expressions:
        expr = normalize_identifiers(exp.column(expr) if isinstance(expr, exp.Identifier) else expr)
        expr.meta["dialect"] = dialect
        results.append(expr)

    return results


def parse_bool(v: t.Any) -> bool:
    if isinstance(v, exp.Boolean):
        return v.this
    if isinstance(v, exp.Expression):
        return str_to_bool(v.name)
    return str_to_bool(str(v or ""))


@field_validator_v1_args
def parse_properties(cls: t.Type, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[exp.Tuple]:
    if v is None:
        return v

    dialect = values.get("dialect")
    if isinstance(v, str):
        v = parse_one(v, dialect=dialect)
    if isinstance(v, (exp.Array, exp.Paren, exp.Tuple)):
        eq_expressions: t.List[exp.Expression] = (
            [v.unnest()] if isinstance(v, exp.Paren) else v.expressions
        )

        for eq_expr in eq_expressions:
            if not isinstance(eq_expr, exp.EQ):
                raise ConfigError(
                    f"Invalid property '{eq_expr.sql(dialect=dialect)}'. "
                    "Properties must be specified as key-value pairs <key> = <value>. "
                )

        properties = (
            exp.Tuple(expressions=eq_expressions) if isinstance(v, (exp.Paren, exp.Array)) else v
        )
    elif isinstance(v, dict):
        properties = exp.Tuple(
            expressions=[exp.Literal.string(key).eq(value) for key, value in v.items()]
        )
    else:
        raise SQLMeshError(f"Unexpected properties '{v}'")

    properties.meta["dialect"] = dialect
    return properties


expression_validator = field_validator(
    "query",
    "expressions_",
    "pre_statements_",
    "post_statements_",
    "unique_key",
    mode="before",
    check_fields=False,
)(parse_expression)


bool_validator = field_validator(
    "skip",
    "blocking",
    "forward_only",
    "disable_restatement",
    "insert_overwrite",
    "allow_partials",
    mode="before",
    check_fields=False,
)(parse_bool)


properties_validator = field_validator(
    "table_properties_",
    "session_properties_",
    mode="before",
    check_fields=False,
)(parse_properties)
