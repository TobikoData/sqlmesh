from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.dialect import normalize_model_name, parse_one
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
            parse_one(e, dialect=dialect) if not isinstance(e, exp.Expression) else e for e in v
        ]

    if isinstance(v, str):
        return parse_one(v, dialect=dialect)

    if not v:
        raise ConfigError(f"Could not parse {v}")

    return v


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


def default_catalog(cls: t.Type, v: t.Any) -> t.Optional[str]:
    if v is None:
        return None
    # If v is an expression then we will return expression as sql without a dialect
    return str(v)


@field_validator_v1_args
def depends_on(cls: t.Type, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[t.Set[str]]:
    dialect = values.get("dialect")
    default_catalog = values.get("default_catalog")

    if isinstance(v, exp.Paren):
        v = v.unnest()

    if isinstance(v, (exp.Array, exp.Tuple)):
        return {
            normalize_model_name(
                table.name if table.is_string else table,
                default_catalog=default_catalog,
                dialect=dialect,
            )
            for table in v.expressions
        }
    if isinstance(v, (exp.Table, exp.Column)):
        return {normalize_model_name(v, default_catalog=default_catalog, dialect=dialect)}
    if hasattr(v, "__iter__") and not isinstance(v, str):
        return {
            normalize_model_name(name, default_catalog=default_catalog, dialect=dialect)
            for name in v
        }

    return v


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
    "enabled",
    mode="before",
    check_fields=False,
)(parse_bool)


properties_validator = field_validator(
    "physical_properties_",
    "virtual_properties_",
    "session_properties_",
    "materialization_properties_",
    mode="before",
    check_fields=False,
)(parse_properties)


default_catalog_validator = field_validator(
    "default_catalog",
    mode="before",
    check_fields=False,
)(default_catalog)


depends_on_validator = field_validator(
    "depends_on_",
    mode="before",
    check_fields=False,
)(depends_on)
