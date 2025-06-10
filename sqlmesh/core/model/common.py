from __future__ import annotations

import ast
import typing as t
from pathlib import Path

from astor import to_source
from difflib import get_close_matches
from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroRegistry, MacroStrTemplate
from sqlmesh.utils import str_to_bool
from sqlmesh.utils.errors import ConfigError, SQLMeshError, raise_config_error
from sqlmesh.utils.metaprogramming import (
    Executable,
    SqlValue,
    build_env,
    prepare_env,
    serialize_env,
)
from sqlmesh.utils.pydantic import PydanticModel, ValidationInfo, field_validator

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlmesh.utils import registry_decorator
    from sqlmesh.utils.jinja import MacroReference

    MacroCallable = registry_decorator


def make_python_env(
    expressions: t.Union[
        exp.Expression,
        t.List[t.Union[exp.Expression, t.Tuple[exp.Expression, bool]]],
    ],
    jinja_macro_references: t.Optional[t.Set[MacroReference]],
    module_path: Path,
    macros: MacroRegistry,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    used_variables: t.Optional[t.Set[str]] = None,
    path: t.Optional[str | Path] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    strict_resolution: bool = True,
    blueprint_variables: t.Optional[t.Dict[str, t.Any]] = None,
    dialect: DialectType = None,
) -> t.Dict[str, Executable]:
    python_env = {} if python_env is None else python_env
    variables = variables or {}
    env: t.Dict[str, t.Tuple[t.Any, t.Optional[bool]]] = {}
    used_macros: t.Dict[
        str,
        t.Tuple[t.Union[Executable | MacroCallable], t.Optional[bool]],
    ] = {}
    used_variables = (used_variables or set()).copy()

    expressions = ensure_list(expressions)
    for expression_metadata in expressions:
        if isinstance(expression_metadata, tuple):
            expression, is_metadata = expression_metadata
        else:
            expression, is_metadata = expression_metadata, None

        if isinstance(expression, d.Jinja):
            continue

        for macro_func_or_var in expression.find_all(d.MacroFunc, d.MacroVar, exp.Identifier):
            if macro_func_or_var.__class__ is d.MacroFunc:
                name = macro_func_or_var.this.name.lower()
                if name not in macros:
                    continue

                # If this macro has been seen before as a non-metadata macro, prioritize that
                used_macros[name] = (
                    macros[name],
                    used_macros.get(name, (None, is_metadata))[1] and is_metadata,
                )
                if name == c.VAR:
                    args = macro_func_or_var.this.expressions
                    if len(args) < 1:
                        raise_config_error("Macro VAR requires at least one argument", path)
                    if not args[0].is_string:
                        raise_config_error(
                            f"The variable name must be a string literal, '{args[0].sql()}' was given instead",
                            path,
                        )
                    used_variables.add(args[0].this.lower())
            elif macro_func_or_var.__class__ is d.MacroVar:
                name = macro_func_or_var.name.lower()
                if name in macros:
                    # If this macro has been seen before as a non-metadata macro, prioritize that
                    used_macros[name] = (
                        macros[name],
                        used_macros.get(name, (None, is_metadata))[1] and is_metadata,
                    )
                elif name in variables:
                    used_variables.add(name)
            elif (
                isinstance(macro_func_or_var, (exp.Identifier, d.MacroStrReplace, d.MacroSQL))
            ) and "@" in macro_func_or_var.name:
                for _, identifier, braced_identifier, _ in MacroStrTemplate.pattern.findall(
                    macro_func_or_var.name
                ):
                    var_name = braced_identifier or identifier
                    if var_name in variables:
                        used_variables.add(var_name)

    for macro_ref in jinja_macro_references or set():
        if macro_ref.package is None and macro_ref.name in macros:
            used_macros[macro_ref.name] = (macros[macro_ref.name], None)

    for name, (used_macro, is_metadata) in used_macros.items():
        if isinstance(used_macro, Executable):
            python_env[name] = used_macro
        elif not hasattr(used_macro, c.SQLMESH_BUILTIN) and name not in python_env:
            build_env(
                used_macro.func,
                env=env,
                name=name,
                path=module_path,
                is_metadata_obj=is_metadata,
            )

    python_env.update(serialize_env(env, path=module_path))
    return _add_variables_to_python_env(
        python_env,
        used_variables,
        variables,
        blueprint_variables=blueprint_variables,
        dialect=dialect,
        strict_resolution=strict_resolution,
    )


def _add_variables_to_python_env(
    python_env: t.Dict[str, Executable],
    used_variables: t.Optional[t.Set[str]],
    variables: t.Optional[t.Dict[str, t.Any]],
    strict_resolution: bool = True,
    blueprint_variables: t.Optional[t.Dict[str, t.Any]] = None,
    dialect: DialectType = None,
) -> t.Dict[str, Executable]:
    _, python_used_variables = parse_dependencies(
        python_env,
        None,
        strict_resolution=strict_resolution,
        variables=variables,
        blueprint_variables=blueprint_variables,
    )
    used_variables = (used_variables or set()) | python_used_variables

    variables = {k: v for k, v in (variables or {}).items() if k in used_variables}
    if variables:
        python_env[c.SQLMESH_VARS] = Executable.value(variables)

    if blueprint_variables:
        blueprint_variables = {
            k: SqlValue(sql=v.sql(dialect=dialect)) if isinstance(v, exp.Expression) else v
            for k, v in blueprint_variables.items()
        }
        python_env[c.SQLMESH_BLUEPRINT_VARS] = Executable.value(blueprint_variables)

    return python_env


def parse_dependencies(
    python_env: t.Dict[str, Executable],
    entrypoint: t.Optional[str],
    strict_resolution: bool = True,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    blueprint_variables: t.Optional[t.Dict[str, t.Any]] = None,
) -> t.Tuple[t.Set[str], t.Set[str]]:
    """
    Parses the source of a model function and finds upstream table dependencies
    and referenced variables based on calls to context / evaluator.

    Args:
        python_env: A dictionary of Python definitions.
        entrypoint: The name of the function.
        strict_resolution: If true, the arguments of `table` and `resolve_table` calls must
            be resolvable at parse time, otherwise an exception will be raised.
        variables: The variables available to the python environment.
        blueprint_variables: The blueprint variables available to the python environment.

    Returns:
        A tuple containing the set of upstream table dependencies and the set of referenced variables.
    """

    class VariableResolutionContext:
        """This enables calls like `resolve_table` to reference `var()` and `blueprint_var()`."""

        @staticmethod
        def var(var_name: str, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
            return (variables or {}).get(var_name.lower(), default)

        @staticmethod
        def blueprint_var(var_name: str, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
            return (blueprint_variables or {}).get(var_name.lower(), default)

    env = prepare_env(python_env)
    local_env = dict.fromkeys(("context", "evaluator"), VariableResolutionContext)

    depends_on = set()
    used_variables = set()

    for executable in python_env.values():
        if not executable.is_definition:
            continue
        for node in ast.walk(ast.parse(executable.payload)):
            if isinstance(node, ast.Call):
                func = node.func
                if not isinstance(func, ast.Attribute) or not isinstance(func.value, ast.Name):
                    continue

                def get_first_arg(keyword_arg_name: str) -> t.Any:
                    if node.args:
                        first_arg: t.Optional[ast.expr] = node.args[0]
                    else:
                        first_arg = next(
                            (
                                keyword.value
                                for keyword in node.keywords
                                if keyword.arg == keyword_arg_name
                            ),
                            None,
                        )

                    try:
                        expression = to_source(first_arg)
                        return eval(expression, env, local_env)
                    except Exception:
                        if strict_resolution:
                            raise ConfigError(
                                f"Error resolving dependencies for '{executable.path}'. "
                                f"Argument '{expression.strip()}' must be resolvable at parse time."
                            )

                if func.value.id == "context" and func.attr in ("table", "resolve_table"):
                    depends_on.add(get_first_arg("model_name"))
                elif func.value.id in ("context", "evaluator") and func.attr == c.VAR:
                    used_variables.add(get_first_arg("var_name").lower())
            elif (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in ("context", "evaluator")
                and node.attr == c.GATEWAY
            ):
                # Check whether the gateway attribute is referenced.
                used_variables.add(c.GATEWAY)
            elif isinstance(node, ast.FunctionDef) and node.name == entrypoint:
                used_variables.update(
                    [
                        arg.arg
                        for arg in [*node.args.args, *node.args.kwonlyargs]
                        if arg.arg != "context"
                    ]
                )

    return depends_on, used_variables


def validate_extra_and_required_fields(
    klass: t.Type[PydanticModel],
    provided_fields: t.Set[str],
    entity_name: str,
) -> None:
    missing_required_fields = klass.missing_required_fields(provided_fields)
    if missing_required_fields:
        field_names = "'" + "', '".join(missing_required_fields) + "'"
        raise_config_error(
            f"Please add required field{'s' if len(missing_required_fields) > 1 else ''} {field_names} to the {entity_name}."
        )

    extra_fields = klass.extra_fields(provided_fields)
    if extra_fields:
        extra_field_names = "'" + "', '".join(extra_fields) + "'"

        all_fields = klass.all_fields()
        close_matches = {}
        for field in extra_fields:
            matches = get_close_matches(field, all_fields, n=1)
            if matches:
                close_matches[field] = matches[0]

        if len(close_matches) == 1:
            similar_msg = ". Did you mean " + "'" + "', '".join(close_matches.values()) + "'?"
        else:
            similar = [
                f"- {field}: Did you mean '{match}'?" for field, match in close_matches.items()
            ]
            similar_msg = "\n\n  " + "\n  ".join(similar) if similar else ""

        raise_config_error(
            f"Invalid field name{'s' if len(extra_fields) > 1 else ''} present in the {entity_name}: {extra_field_names}{similar_msg}"
        )


def single_value_or_tuple(values: t.Sequence) -> exp.Identifier | exp.Tuple:
    return (
        exp.to_identifier(values[0])
        if len(values) == 1
        else exp.Tuple(expressions=[exp.to_identifier(v) for v in values])
    )


def parse_expression(
    cls: t.Type,
    v: t.Union[t.List[str], t.List[exp.Expression], str, exp.Expression, t.Callable, None],
    info: t.Optional[ValidationInfo],
) -> t.List[exp.Expression] | exp.Expression | t.Callable | None:
    """Helper method to deserialize SQLGlot expressions in Pydantic Models."""
    if v is None:
        return None

    if callable(v):
        return v

    dialect = info.data.get("dialect") if info else ""

    if isinstance(v, list):
        return [
            e if isinstance(e, exp.Expression) else d.parse_one(e, dialect=dialect)
            for e in v
            if not isinstance(e, exp.Semicolon)
        ]

    if isinstance(v, str):
        return d.parse_one(v, dialect=dialect)

    if not v:
        raise ConfigError(f"Could not parse {v}")

    return v


def parse_bool(v: t.Any) -> bool:
    if isinstance(v, exp.Boolean):
        return v.this
    if isinstance(v, exp.Expression):
        return str_to_bool(v.name)
    return str_to_bool(str(v or ""))


def parse_properties(
    cls: t.Type, v: t.Any, info: t.Optional[ValidationInfo]
) -> t.Optional[exp.Tuple]:
    if v is None:
        return v

    dialect = info.data.get("dialect") if info else ""

    if isinstance(v, str):
        v = d.parse_one(v, dialect=dialect)
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


def depends_on(cls: t.Type, v: t.Any, info: ValidationInfo) -> t.Optional[t.Set[str]]:
    dialect = info.data.get("dialect")
    default_catalog = info.data.get("default_catalog")

    if isinstance(v, exp.Paren):
        v = v.unnest()

    if isinstance(v, (exp.Array, exp.Tuple)):
        return {
            d.normalize_model_name(
                table.name if table.is_string else table,
                default_catalog=default_catalog,
                dialect=dialect,
            )
            for table in v.expressions
        }
    if isinstance(v, (exp.Table, exp.Column)):
        return {d.normalize_model_name(v, default_catalog=default_catalog, dialect=dialect)}
    if hasattr(v, "__iter__") and not isinstance(v, str):
        return {
            d.normalize_model_name(name, default_catalog=default_catalog, dialect=dialect)
            for name in v
        }

    return v


def sort_python_env(python_env: t.Dict[str, Executable]) -> t.List[t.Tuple[str, Executable]]:
    """Returns the python env sorted."""
    return sorted(python_env.items(), key=lambda x: (x[1].kind, x[0]))


def sorted_python_env_payloads(python_env: t.Dict[str, Executable]) -> t.List[str]:
    """Returns the payloads of the sorted python env."""

    def _executable_to_str(k: str, v: Executable) -> str:
        result = f"# {v.path}\n" if v.path is not None else ""
        if v.is_import or v.is_definition:
            result += v.payload
        else:
            result += f"{k} = {v.payload}"
        return result

    return [_executable_to_str(k, v) for k, v in sort_python_env(python_env)]


expression_validator: t.Callable = field_validator(
    "query",
    "expressions_",
    "pre_statements_",
    "post_statements_",
    "on_virtual_update_",
    "unique_key",
    mode="before",
    check_fields=False,
)(parse_expression)


bool_validator: t.Callable = field_validator(
    "skip",
    "blocking",
    "forward_only",
    "disable_restatement",
    "insert_overwrite",
    "allow_partials",
    "enabled",
    "optimize_query",
    "formatting",
    mode="before",
    check_fields=False,
)(parse_bool)


properties_validator: t.Callable = field_validator(
    "physical_properties_",
    "virtual_properties_",
    "materialization_properties_",
    mode="before",
    check_fields=False,
)(parse_properties)


default_catalog_validator: t.Callable = field_validator(
    "default_catalog",
    mode="before",
    check_fields=False,
)(default_catalog)


depends_on_validator: t.Callable = field_validator(
    "depends_on_",
    mode="before",
    check_fields=False,
)(depends_on)
