from __future__ import annotations

import inspect
import logging
import sys
import types
import typing as t
from enum import Enum
from functools import reduce
from itertools import chain
from pathlib import Path
from string import Template
from datetime import datetime

import sqlglot
from jinja2 import Environment
from sqlglot import Generator, exp, parse_one
from sqlglot.executor.env import ENV
from sqlglot.executor.python import Python
from sqlglot.helper import csv, ensure_collection
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import (
    SQLMESH_MACRO_PREFIX,
    Dialect,
    MacroDef,
    MacroFunc,
    MacroSQL,
    MacroStrReplace,
    MacroVar,
    StagedFilePath,
    normalize_model_name,
)
from sqlmesh.utils import (
    DECORATOR_RETURN_TYPE,
    UniqueKeyDict,
    columns_to_types_all_known,
    registry_decorator,
)
from sqlmesh.utils.errors import MacroEvalError, SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, has_jinja
from sqlmesh.utils.metaprogramming import Executable, prepare_env, print_exception

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.snapshot import Snapshot


if sys.version_info >= (3, 10):
    UNION_TYPES = (t.Union, types.UnionType)
else:
    UNION_TYPES = (t.Union,)


logger = logging.getLogger(__name__)


class RuntimeStage(Enum):
    LOADING = "loading"
    CREATING = "creating"
    EVALUATING = "evaluating"
    TESTING = "testing"


class MacroStrTemplate(Template):
    delimiter = SQLMESH_MACRO_PREFIX


EXPRESSIONS_NAME_MAP = {}
SQL = t.NewType("SQL", str)

SUPPORTED_TYPES = {
    "t": t,
    "typing": t,
    "List": t.List,
    "Tuple": t.Tuple,
    "Union": t.Union,
}

for klass in sqlglot.Parser.EXPRESSION_PARSERS:
    name = klass if isinstance(klass, str) else klass.__name__  # type: ignore
    EXPRESSIONS_NAME_MAP[name.lower()] = name


def _macro_sql(sql: str, into: t.Optional[str] = None) -> str:
    args = [_macro_str_replace(sql)]
    if into in EXPRESSIONS_NAME_MAP:
        args.append(f"into=exp.{EXPRESSIONS_NAME_MAP[into]}")
    return f"self.parse_one({', '.join(args)})"


def _macro_func_sql(self: Generator, e: exp.Expression) -> str:
    func = e.this

    if isinstance(func, exp.Anonymous):
        return f"""self.send({csv("'" + func.name + "'", self.expressions(func))})"""
    return self.sql(func)


def _macro_str_replace(text: str) -> str:
    """Stringifies python code for variable replacement
    Args:
        text: text string
    Returns:
        Stringified python code to execute variable replacement
    """
    return f"self.template({text}, locals())"


class MacroDialect(Python):
    class Generator(Python.Generator):
        TRANSFORMS = {
            **Python.Generator.TRANSFORMS,  # type: ignore
            exp.Column: lambda self, e: f"exp.to_column('{self.sql(e, 'this')}')",
            exp.Lambda: lambda self, e: f"lambda {self.expressions(e)}: {self.sql(e, 'this')}",
            MacroFunc: _macro_func_sql,
            MacroSQL: lambda self, e: _macro_sql(self.sql(e, "this"), e.args.get("into")),
            MacroStrReplace: lambda self, e: _macro_str_replace(self.sql(e, "this")),
        }


class MacroEvaluator:
    """The class responsible for evaluating SQLMesh Macros/SQL.

    SQLMesh supports special preprocessed SQL prefixed with `@`. Although it provides similar power to
    traditional methods like string templating, there is semantic understanding of SQL which prevents
    common errors like leading/trailing commas, syntax errors, etc.

    SQLMesh SQL allows for macro variables and macro functions. Macro variables take the form of @variable. These are used for variable substitution.

    SELECT * FROM foo WHERE ds BETWEEN @start_date AND @end_date

    Macro variables can be defined with a special macro function.

    @DEF(start_date, '2021-01-01')

    Args:
        dialect: Dialect of the SQL to evaluate.
        python_env: Serialized Python environment.
    """

    def __init__(
        self,
        dialect: DialectType = "",
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        jinja_env: t.Optional[Environment] = None,
        schema: t.Optional[MappingSchema] = None,
        runtime_stage: RuntimeStage = RuntimeStage.LOADING,
        resolve_tables: t.Optional[t.Callable[[exp.Expression], exp.Expression]] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        default_catalog: t.Optional[str] = None,
        path: Path = Path(),
    ):
        self.dialect = dialect
        self.generator = MacroDialect().generator()
        self.locals: t.Dict[str, t.Any] = {
            "runtime_stage": runtime_stage.value,
            "default_catalog": default_catalog,
        }
        self.env = {
            **ENV,
            "self": self,
            "SQL": SQL,
            "MacroEvaluator": MacroEvaluator,
        }
        self.python_env = python_env or {}
        self._jinja_env: t.Optional[Environment] = jinja_env
        self.macros = {normalize_macro_name(k): v.func for k, v in macro.get_registry().items()}
        self._schema = schema
        self._resolve_tables = resolve_tables
        self.columns_to_types_called = False
        self._snapshots = snapshots if snapshots is not None else {}
        self.default_catalog = default_catalog
        self._path = path

        prepare_env(self.python_env, self.env)
        for k, v in self.python_env.items():
            if v.is_definition:
                self.macros[normalize_macro_name(k)] = self.env[v.name or k]
            elif v.is_import and getattr(self.env.get(k), c.SQLMESH_MACRO, None):
                self.macros[normalize_macro_name(k)] = self.env[k]
            elif v.is_value:
                self.locals[k] = self.env[k]

    def send(
        self, name: str, *args: t.Any, **kwargs: t.Any
    ) -> t.Union[None, exp.Expression, t.List[exp.Expression]]:
        func = self.macros.get(normalize_macro_name(name))

        if not callable(func):
            raise SQLMeshError(f"Macro '{name}' does not exist.")

        try:
            # Bind the macro's actual parameters to its formal parameters
            sig = inspect.signature(func)
            bound = sig.bind(self, *args, **kwargs)
            bound.apply_defaults()
        except Exception as e:
            print_exception(e, self.python_env)
            raise MacroEvalError("Error trying to eval macro.") from e

        try:
            annotations = t.get_type_hints(func, localns=SUPPORTED_TYPES)
        except NameError:  # forward references aren't handled
            annotations = {}

        # If the macro is annotated, we try coerce the actual parameters to the corresponding types
        if annotations:
            for arg, value in bound.arguments.items():
                typ = annotations.get(arg)
                if not typ:
                    continue

                # Changes to bound.arguments will reflect in bound.args and bound.kwargs
                # https://docs.python.org/3/library/inspect.html#inspect.BoundArguments.arguments
                param = sig.parameters[arg]
                if param.kind is inspect.Parameter.VAR_POSITIONAL:
                    bound.arguments[arg] = tuple(self._coerce(v, typ) for v in value)
                elif param.kind is inspect.Parameter.VAR_KEYWORD:
                    bound.arguments[arg] = {k: self._coerce(v, typ) for k, v in value.items()}
                else:
                    bound.arguments[arg] = self._coerce(value, typ)

        try:
            return func(*bound.args, **bound.kwargs)
        except Exception as e:
            print_exception(e, self.python_env)
            raise MacroEvalError("Error trying to eval macro.") from e

    def transform(
        self, expression: exp.Expression
    ) -> exp.Expression | t.List[exp.Expression] | None:
        changed = False

        def evaluate_macros(
            node: exp.Expression,
        ) -> exp.Expression | t.List[exp.Expression] | None:
            nonlocal changed

            if isinstance(node, MacroVar):
                changed = True
                variables = self.locals.get(c.SQLMESH_VARS, {})
                if node.name not in self.locals and node.name.lower() not in variables:
                    if not isinstance(node.parent, StagedFilePath):
                        raise SQLMeshError(f"Macro variable '{node.name}' is undefined.")

                    return node

                value = self.locals.get(node.name, variables.get(node.name.lower()))
                if isinstance(value, list):
                    return exp.convert(
                        tuple(
                            self.transform(v) if isinstance(v, exp.Expression) else v for v in value
                        )
                    )
                return exp.convert(
                    self.transform(value) if isinstance(value, exp.Expression) else value
                )
            if isinstance(node, exp.Identifier) and "@" in node.this:
                text = self.template(node.this, self.locals)
                if node.this != text:
                    changed = True
                    node.args["this"] = text
                    return node
            if node.is_string:
                text = node.this
                if has_jinja(text):
                    changed = True
                    node.set("this", self.jinja_env.from_string(node.this).render())
                return node
            if isinstance(node, MacroFunc):
                changed = True
                return self.evaluate(node)
            return node

        transformed = exp.replace_tree(
            expression.copy(), evaluate_macros, prune=lambda n: isinstance(n, exp.Lambda)
        )

        if changed:
            # the transformations could have corrupted the ast, turning this into sql and reparsing ensures
            # that the ast is correct
            if isinstance(transformed, list):
                return [
                    self.parse_one(node.sql(dialect=self.dialect, copy=False))
                    for node in transformed
                ]
            elif isinstance(transformed, exp.Expression):
                return self.parse_one(transformed.sql(dialect=self.dialect, copy=False))

        return transformed

    def template(self, text: t.Any, local_variables: t.Dict[str, t.Any]) -> str:
        """Substitute @vars with locals.

        Args:
            text: The string to do substitition on.
            local_variables: Local variables in the context so that lambdas can be used.

        Returns:
           The rendered string.
        """
        mapping = {}

        variables = self.locals.get(c.SQLMESH_VARS, {})

        for k, v in chain(variables.items(), self.locals.items(), local_variables.items()):
            # try to convert all variables into sqlglot expressions
            # because they're going to be converted into strings in sql
            # we use bare Exception instead of ValueError because there's
            # a recursive error with MagicMock.
            # we don't convert strings because that would result in adding quotes
            if not isinstance(v, str):
                try:
                    v = exp.convert(v)
                except Exception:
                    pass

            if isinstance(v, exp.Expression):
                v = v.sql(dialect=self.dialect)
            mapping[k] = v

        return MacroStrTemplate(str(text)).safe_substitute(mapping)

    def evaluate(self, node: MacroFunc) -> exp.Expression | t.List[exp.Expression] | None:
        if isinstance(node, MacroDef):
            if isinstance(node.expression, exp.Lambda):
                _, fn = _norm_var_arg_lambda(self, node.expression)
                self.macros[normalize_macro_name(node.name)] = lambda _, *args: fn(
                    args[0] if len(args) == 1 else exp.Tuple(expressions=list(args))
                )
            else:
                self.locals[node.name] = self.transform(node.expression)
            return node

        if isinstance(node, (MacroSQL, MacroStrReplace)):
            result: t.Optional[exp.Expression | t.List[exp.Expression]] = exp.convert(
                self.eval_expression(node)
            )
        else:
            func = t.cast(exp.Anonymous, node.this)

            args = []
            kwargs = {}
            for e in func.expressions:
                if isinstance(e, exp.PropertyEQ):
                    kwargs[e.this.name] = e.expression
                else:
                    if kwargs:
                        raise MacroEvalError(
                            "Positional argument cannot follow keyword argument.\n  "
                            f"{func.sql(dialect=self.dialect)} at '{self._path}'"
                        )

                    args.append(e)

            result = self.send(func.name, *args, **kwargs)

        if result is None:
            return None

        if isinstance(result, (tuple, list)):
            return [self.parse_one(item) for item in result if item is not None]
        return self.parse_one(result)

    def eval_expression(self, node: t.Any) -> t.Any:
        """Converts a SQLGlot expression into executable Python code and evals it.

        If the node is not an expression, it will simply be returned.

        Args:
            node: expression
        Returns:
            The return value of the evaled Python Code.
        """
        if not isinstance(node, exp.Expression):
            return node
        code = node.sql()
        try:
            code = self.generator.generate(node)
            return eval(code, self.env, self.locals)
        except Exception as e:
            print_exception(e, self.python_env)
            raise MacroEvalError(
                f"Error trying to eval macro.\n\nGenerated code: {code}\n\nOriginal sql: {node}"
            ) from e

    def parse_one(
        self, sql: str | exp.Expression, into: t.Optional[exp.IntoType] = None, **opts: t.Any
    ) -> exp.Expression:
        """Parses the given SQL string and returns a syntax tree for the first
        parsed SQL statement.

        Args:
            sql: the SQL code or expression to parse.
            into: the Expression to parse into
            **opts: other options

        Returns:
            Expression: the syntax tree for the first parsed statement
        """
        return sqlglot.maybe_parse(sql, dialect=self.dialect, into=into, **opts)

    @property
    def jinja_env(self) -> Environment:
        if not self._jinja_env:
            jinja_env_methods = {**self.locals, **self.env}
            del jinja_env_methods["self"]
            self._jinja_env = JinjaMacroRegistry().build_environment(**jinja_env_methods)
        return self._jinja_env

    def columns_to_types(self, model_name: TableName | exp.Column) -> t.Dict[str, exp.DataType]:
        """Returns the columns-to-types mapping corresponding to the specified model."""

        # We only return this dummy schema at load time, because if we don't actually know the
        # target model's schema at creation/evaluation time, returning a dummy schema could lead
        # to unintelligible errors when the query is executed
        if (self._schema is None or self._schema.empty) and self.runtime_stage == "loading":
            self.columns_to_types_called = True
            return {"__schema_unavailable_at_load__": exp.DataType.build("unknown")}

        normalized_model_name = normalize_model_name(
            model_name,
            default_catalog=self.default_catalog,
            dialect=self.dialect,
        )
        model_name = exp.to_table(normalized_model_name)

        columns_to_types = (
            self._schema.find(model_name, ensure_data_types=True) if self._schema else None
        )
        if columns_to_types is None:
            snapshot = self.get_snapshot(model_name)
            if snapshot and snapshot.node.is_model:
                columns_to_types = snapshot.node.columns_to_types  # type: ignore

        if columns_to_types is None:
            raise SQLMeshError(f"Schema for model '{model_name}' can't be statically determined.")

        return columns_to_types

    def get_snapshot(self, model_name: TableName | exp.Column) -> t.Optional[Snapshot]:
        """Returns the snapshot that corresponds to the given model name."""
        return self._snapshots.get(
            normalize_model_name(
                model_name,
                default_catalog=self.default_catalog,
                dialect=self.dialect,
            )
        )

    def resolve_tables(self, query: exp.Expression) -> exp.Expression:
        """Resolves queries with references to SQLMesh model names to their physical tables."""
        if not self._resolve_tables:
            raise SQLMeshError(
                "Macro evaluator not properly initialized with resolve_tables lambda."
            )
        return self._resolve_tables(query)

    @property
    def runtime_stage(self) -> RuntimeStage:
        """Returns the current runtime stage of the macro evaluation."""
        return self.locals["runtime_stage"]

    @property
    def engine_adapter(self) -> EngineAdapter:
        engine_adapter = self.locals.get("engine_adapter")
        if not engine_adapter:
            raise SQLMeshError(
                "The engine adapter is not available while models are loading."
                " You can gate these calls by checking in Python: evaluator.runtime_stage != 'loading' or SQL: @runtime_stage <> 'loading'."
            )
        return self.locals["engine_adapter"]

    @property
    def gateway(self) -> t.Optional[str]:
        """Returns the gateway name."""
        return self.var(c.GATEWAY)

    def var(self, var_name: str, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
        """Returns the value of the specified variable, or the default value if it doesn't exist."""
        return (self.locals.get(c.SQLMESH_VARS) or {}).get(var_name.lower(), default)

    def _coerce(self, expr: exp.Expression, typ: t.Any, strict: bool = False) -> t.Any:
        """Coerces the given expression to the specified type on a best-effort basis."""
        base_err_msg = f"Failed to coerce expression '{expr}' to type '{typ}'."
        try:
            if typ is None or typ is t.Any:
                return expr
            base = t.get_origin(typ) or typ

            # We need to handle Union and TypeVars first since we cannot use isinstance with it
            if base in UNION_TYPES:
                for branch in t.get_args(typ):
                    try:
                        return self._coerce(expr, branch, True)
                    except Exception:
                        pass
                raise SQLMeshError(base_err_msg)
            if base is SQL and isinstance(expr, exp.Expression):
                return expr.sql(self.dialect)

            if isinstance(expr, base):
                return expr
            if issubclass(base, exp.Expression):
                d = Dialect.get_or_raise(self.dialect)
                into = base if base in d.parser_class.EXPRESSION_PARSERS else None
                if into is None:
                    if isinstance(expr, exp.Literal):
                        coerced = parse_one(expr.this)
                    else:
                        raise SQLMeshError(
                            f"{base_err_msg} Coercion to {base} requires a literal expression."
                        )
                else:
                    coerced = parse_one(
                        expr.this if isinstance(expr, exp.Literal) else expr.sql(), into=into
                    )
                if isinstance(coerced, base):
                    return coerced
                raise SQLMeshError(base_err_msg)

            if base in (int, float, str) and isinstance(expr, exp.Literal):
                return base(expr.this)
            if base is str and isinstance(expr, exp.Column) and not expr.table:
                return expr.name
            if base is bool and isinstance(expr, exp.Boolean):
                return expr.this
            # if base is str and isinstance(expr, exp.Expression):
            #    return expr.sql(self.dialect)
            if base is tuple and isinstance(expr, (exp.Tuple, exp.Array)):
                generic = t.get_args(typ)
                if not generic:
                    return tuple(expr.expressions)
                if generic[-1] is ...:
                    return tuple(self._coerce(expr, generic[0]) for expr in expr.expressions)
                elif len(generic) == len(expr.expressions):
                    return tuple(
                        self._coerce(expr, generic[i]) for i, expr in enumerate(expr.expressions)
                    )
                raise SQLMeshError(f"{base_err_msg} Expected {len(generic)} items.")
            if base is list and isinstance(expr, (exp.Array, exp.Tuple)):
                generic = t.get_args(typ)
                if not generic:
                    return expr.expressions
                return [self._coerce(expr, generic[0]) for expr in expr.expressions]
            raise SQLMeshError(base_err_msg)
        except Exception:
            if strict:
                raise
            logger.error(
                "Coercion of expression '%s' to type '%s' failed. Using non coerced expression at '%s'",
                expr,
                typ,
                self._path,
            )
            return expr


class macro(registry_decorator):
    """Specifies a function is a macro and registers it the global MACROS registry.

    Registered macros can be referenced in SQL statements to make queries more dynamic/cleaner.

    Example:
        from sqlglot import exp
        from sqlmesh.core.macros import MacroEvaluator, macro

        @macro()
        def add_one(evaluator: MacroEvaluator, column: exp.Literal) -> exp.Add:
            return evaluator.parse_one(f"{column} + 1")

    Args:
        name: A custom name for the macro, the default is the name of the function.
    """

    registry_name = "macros"

    def __init__(self, *args: t.Any, metadata_only: bool = False, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.metadata_only = metadata_only

    def __call__(
        self, func: t.Callable[..., DECORATOR_RETURN_TYPE]
    ) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
        if self.metadata_only:
            setattr(func, c.SQLMESH_METADATA, self.metadata_only)
        wrapper = super().__call__(func)

        # This is used to identify macros at runtime to unwrap during serialization.
        setattr(wrapper, c.SQLMESH_MACRO, True)
        return wrapper


ExecutableOrMacro = t.Union[Executable, macro]
MacroRegistry = UniqueKeyDict[str, ExecutableOrMacro]


def _norm_var_arg_lambda(
    evaluator: MacroEvaluator, func: exp.Lambda, *items: t.Any
) -> t.Tuple[t.Iterable, t.Callable]:
    """
    Converts sql literal array and lambda into actual python iterable + callable.

    In order to support expressions like @EACH([a, b, c], x -> @SQL('@x')), the lambda var x
    needs be passed to the local state.

    Args:
        evaluator: MacroEvaluator that invoked the macro
        func: Lambda SQLGlot expression.
        items: Array or items of SQLGlot expressions.
    """

    def substitute(
        node: exp.Expression, args: t.Dict[str, exp.Expression]
    ) -> exp.Expression | t.List[exp.Expression] | None:
        if isinstance(node, (exp.Identifier, exp.Var)):
            if not isinstance(node.parent, exp.Column):
                name = node.name
                if name in args:
                    return args[name].copy()
                if name in evaluator.locals:
                    return exp.convert(evaluator.locals[name])
            if SQLMESH_MACRO_PREFIX in node.name:
                return node.__class__(
                    this=evaluator.template(node.name, {k: v.name for k, v in args.items()})
                )
        elif isinstance(node, MacroFunc):
            local_copy = evaluator.locals.copy()
            evaluator.locals.update(args)
            result = evaluator.transform(node)
            evaluator.locals = local_copy
            return result
        return node

    if len(items) == 1:
        item = items[0]
        expressions = item.expressions if isinstance(item, (exp.Array, exp.Tuple)) else item
    else:
        expressions = items

    if not callable(func):
        return expressions, lambda args: func.this.transform(
            substitute,
            {
                expression.name: arg
                for expression, arg in zip(
                    func.expressions, args.expressions if isinstance(args, exp.Tuple) else [args]
                )
            },
        )

    return expressions, func


@macro()
def each(
    evaluator: MacroEvaluator,
    *args: t.Any,
) -> t.List[t.Any]:
    """Iterates through items calling func on each.

    If a func call on item returns None, it will be excluded from the list.

    Args:
        evaluator: MacroEvaluator that invoked the macro
        args: The last argument should be a lambda of the form x -> x +1. The first argument can be
            an Array or var args can be used.

    Returns:
        A list of items that is the result of func
    """
    *items, func = args
    items, func = _norm_var_arg_lambda(evaluator, func, *items)  # type: ignore
    return [item for item in map(func, ensure_collection(items)) if item is not None]


@macro("IF")
def if_(
    evaluator: MacroEvaluator,
    condition: t.Any,
    true: t.Any,
    false: t.Any = None,
) -> t.Any:
    """Evaluates a given condition and returns the second argument if true or else the third argument.

    If false is not passed in, the default return value will be None.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> MacroEvaluator().transform(parse_one("@IF('a' = 1, a, b)")).sql()
        'b'

        >>> MacroEvaluator().transform(parse_one("@IF('a' = 1, a)"))
    """

    if evaluator.eval_expression(condition):
        return true
    return false


@macro("REDUCE")
def reduce_(evaluator: MacroEvaluator, *args: t.Any) -> t.Any:
    """Iterates through items applying provided function that takes two arguments
    cumulatively to the items of iterable items, from left to right, so as to reduce
    the iterable to a single item.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@SQL(@REDUCE([100, 200, 300, 400], (x, y) -> x + y))"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        '1000'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        args: The last argument should be a lambda of the form (x, y) -> x + y. The first argument can be
            an Array or var args can be used.
    Returns:
        A single item that is the result of applying func cumulatively to items
    """
    *items, func = args
    items, func = _norm_var_arg_lambda(evaluator, func, *items)  # type: ignore
    return reduce(lambda a, b: func(exp.Tuple(expressions=[a, b])), ensure_collection(items))


@macro("FILTER")
def filter_(evaluator: MacroEvaluator, *args: t.Any) -> t.List[t.Any]:
    """Iterates through items, applying provided function to each item and removing
    all items where the function returns False

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@REDUCE(@FILTER([1, 2, 3], x -> x > 1), (x, y) -> x + y)"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        '2 + 3'

        >>> sql = "@EVAL(@REDUCE(@FILTER([1, 2, 3], x -> x > 1), (x, y) -> x + y))"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        '5'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        args: The last argument should be a lambda of the form x -> x > 1. The first argument can be
            an Array or var args can be used.
    Returns:
        The items for which the func returned True
    """
    *items, func = args
    items, func = _norm_var_arg_lambda(evaluator, func, *items)  # type: ignore
    return list(filter(lambda arg: evaluator.eval_expression(func(arg)), items))


def _optional_expression(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Expression,
) -> t.Optional[exp.Expression]:
    """Inserts expression when the condition is True

    The following examples express the usage of this function in the context of the macros which wrap it.

    Examples:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@WITH(True) all_cities as (select * from city) select all_cities"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'WITH all_cities AS (SELECT * FROM city) SELECT all_cities'
        >>> sql = "select * from city left outer @JOIN(True) country on city.country = country.name"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city LEFT OUTER JOIN country ON city.country = country.name'
        >>> sql = "select * from city @GROUP_BY(True) country, population"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city GROUP BY country, population'
        >>> sql = "select * from city group by country @HAVING(True) population > 100 and country = 'Mexico'"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT * FROM city GROUP BY country HAVING population > 100 AND country = 'Mexico'"

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: SQL expression
    Returns:
        Expression if the conditional is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


with_ = macro("WITH")(_optional_expression)
join = macro("JOIN")(_optional_expression)
where = macro("WHERE")(_optional_expression)
group_by = macro("GROUP_BY")(_optional_expression)
having = macro("HAVING")(_optional_expression)
order_by = macro("ORDER_BY")(_optional_expression)
limit = macro("LIMIT")(_optional_expression)


@macro("eval")
def eval_(evaluator: MacroEvaluator, condition: exp.Condition) -> t.Any:
    """Evaluate the given condition in a Python/SQL interpretor.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@EVAL(1 + 1)"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        '2'
    """
    return evaluator.eval_expression(condition)


# macros with union types need to use t.Union since | isn't available until 3.9
@macro()
def star(
    evaluator: MacroEvaluator,
    relation: exp.Table,
    alias: exp.Column = t.cast(exp.Column, exp.column("")),
    exclude: t.Union[exp.Array, exp.Tuple] = exp.Tuple(expressions=[]),
    prefix: exp.Literal = exp.Literal.string(""),
    suffix: exp.Literal = exp.Literal.string(""),
    quote_identifiers: exp.Boolean = exp.true(),
    except_: t.Union[exp.Array, exp.Tuple] = exp.Tuple(expressions=[]),
) -> t.List[exp.Alias]:
    """Returns a list of projections for the given relation.

    Args:
        evaluator: MacroEvaluator that invoked the macro
        relation: The relation to select star from
        alias: The alias of the relation
        exclude: Columns to exclude
        prefix: A prefix to use for all selections
        suffix: A suffix to use for all selections
        quote_identifiers: Whether or not quote the resulting aliases, defaults to true
        except_: Alias for exclude (TODO: deprecate this, update docs)

    Returns:
        An array of columns.

    Example:
        >>> from sqlglot import parse_one, exp
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @STAR(foo, bar, exclude := [c], prefix := 'baz_') FROM foo AS bar"
        >>> MacroEvaluator(schema=MappingSchema({"foo": {"a": exp.DataType.build("string"), "b": exp.DataType.build("string"), "c": exp.DataType.build("string"), "d": exp.DataType.build("int")}})).transform(parse_one(sql)).sql()
        'SELECT CAST("bar"."a" AS TEXT) AS "baz_a", CAST("bar"."b" AS TEXT) AS "baz_b", CAST("bar"."d" AS INT) AS "baz_d" FROM foo AS bar'
    """
    if alias and not isinstance(alias, (exp.Identifier, exp.Column)):
        raise SQLMeshError(f"Invalid alias '{alias}'. Expected an identifier.")
    if exclude and not isinstance(exclude, (exp.Array, exp.Tuple)):
        raise SQLMeshError(f"Invalid exclude '{exclude}'. Expected an array.")
    if except_ != exp.tuple_():
        logger.warning(
            "The 'except_' argument in @STAR will soon be deprecated. Use 'exclude' instead."
        )
        if not isinstance(exclude, (exp.Array, exp.Tuple)):
            raise SQLMeshError(f"Invalid exclude_ '{exclude}'. Expected an array.")
    if prefix and not isinstance(prefix, exp.Literal):
        raise SQLMeshError(f"Invalid prefix '{prefix}'. Expected a literal.")
    if suffix and not isinstance(suffix, exp.Literal):
        raise SQLMeshError(f"Invalid suffix '{suffix}'. Expected a literal.")
    if not isinstance(quote_identifiers, exp.Boolean):
        raise SQLMeshError(f"Invalid quote_identifiers '{quote_identifiers}'. Expected a boolean.")

    excluded_names = {
        normalize_identifiers(excluded, dialect=evaluator.dialect).name
        for excluded in exclude.expressions or except_.expressions
    }
    quoted = quote_identifiers.this
    table_identifier = alias.name or relation.name

    columns_to_types = {
        k: v for k, v in evaluator.columns_to_types(relation).items() if k not in excluded_names
    }
    if columns_to_types_all_known(columns_to_types):
        return [
            exp.cast(
                exp.column(column, table=table_identifier, quoted=quoted),
                dtype,
                dialect=evaluator.dialect,
            ).as_(f"{prefix.this}{column}{suffix.this}", quoted=quoted)
            for column, dtype in columns_to_types.items()
        ]
    return [
        exp.column(column, table=table_identifier, quoted=quoted).as_(
            f"{prefix.this}{column}{suffix.this}", quoted=quoted
        )
        for column, type_ in columns_to_types.items()
    ]


@macro()
def generate_surrogate_key(
    evaluator: MacroEvaluator,
    *fields: exp.Expression,
    hash_function: exp.Literal = exp.Literal.string("MD5"),
) -> exp.Func:
    """Generates a surrogate key for the given fields.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>>
        >>> sql = "SELECT @GENERATE_SURROGATE_KEY(a, b, c) FROM foo"
        >>> MacroEvaluator(dialect="bigquery").transform(parse_one(sql, dialect="bigquery")).sql("bigquery")
        "SELECT MD5(CONCAT(COALESCE(CAST(a AS STRING), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(b AS STRING), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(c AS STRING), '_sqlmesh_surrogate_key_null_'))) FROM foo"
        >>>
        >>> sql = "SELECT @GENERATE_SURROGATE_KEY(a, b, c, hash_function := 'SHA256') FROM foo"
        >>> MacroEvaluator(dialect="bigquery").transform(parse_one(sql, dialect="bigquery")).sql("bigquery")
        "SELECT SHA256(CONCAT(COALESCE(CAST(a AS STRING), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(b AS STRING), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(c AS STRING), '_sqlmesh_surrogate_key_null_'))) FROM foo"
    """
    string_fields: t.List[exp.Expression] = []
    for i, field in enumerate(fields):
        if i > 0:
            string_fields.append(exp.Literal.string("|"))
        string_fields.append(
            exp.func(
                "COALESCE",
                exp.cast(field, exp.DataType.build("text")),
                exp.Literal.string("_sqlmesh_surrogate_key_null_"),
            )
        )
    return exp.func(
        hash_function.name,
        exp.func("CONCAT", *string_fields),
        dialect=evaluator.dialect,
    )


@macro()
def safe_add(_: MacroEvaluator, *fields: exp.Expression) -> exp.Case:
    """Adds numbers together, substitutes nulls for 0s and only returns null if all fields are null.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @SAFE_ADD(a, b) FROM foo"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE COALESCE(a, 0) + COALESCE(b, 0) END FROM foo'
    """
    return (
        exp.Case()
        .when(exp.and_(*(field.is_(exp.null()) for field in fields)), exp.null())
        .else_(reduce(lambda a, b: a + b, [exp.func("COALESCE", field, 0) for field in fields]))  # type: ignore
    )


@macro()
def safe_sub(_: MacroEvaluator, *fields: exp.Expression) -> exp.Case:
    """Subtract numbers, substitutes nulls for 0s and only returns null if all fields are null.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @SAFE_SUB(a, b) FROM foo"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE COALESCE(a, 0) - COALESCE(b, 0) END FROM foo'
    """
    return (
        exp.Case()
        .when(exp.and_(*(field.is_(exp.null()) for field in fields)), exp.null())
        .else_(reduce(lambda a, b: a - b, [exp.func("COALESCE", field, 0) for field in fields]))  # type: ignore
    )


@macro()
def safe_div(_: MacroEvaluator, numerator: exp.Expression, denominator: exp.Expression) -> exp.Div:
    """Divides numbers, returns null if the denominator is 0.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @SAFE_DIV(a, b) FROM foo"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT a / NULLIF(b, 0) FROM foo'
    """
    return numerator / exp.func("NULLIF", denominator, 0)


@macro()
def union(
    evaluator: MacroEvaluator,
    type_: exp.Literal = exp.Literal.string("ALL"),
    *tables: exp.Table,
) -> exp.Query:
    """Returns a UNION of the given tables. Only choosing columns that have the same name and type.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@UNION('distinct', foo, bar)"
        >>> MacroEvaluator(schema=MappingSchema({"foo": {"a": "int", "b": "string", "c": "string"}, "bar": {"c": "string", "a": "int", "b": "int"}})).transform(parse_one(sql)).sql()
        'SELECT CAST(a AS INT) AS a, CAST(c AS TEXT) AS c FROM foo UNION SELECT CAST(a AS INT) AS a, CAST(c AS TEXT) AS c FROM bar'
    """
    kind = type_.name.upper()
    if kind not in ("ALL", "DISTINCT"):
        raise SQLMeshError(f"Invalid type '{type_}'. Expected 'ALL' or 'DISTINCT'.")

    columns = {
        column
        for column, _ in reduce(
            lambda a, b: a & b,  # type: ignore
            (evaluator.columns_to_types(table).items() for table in tables),
        )
    }

    projections = [
        exp.cast(column, type_, dialect=evaluator.dialect).as_(column)
        for column, type_ in evaluator.columns_to_types(tables[0]).items()
        if column in columns
    ]

    return reduce(
        lambda a, b: a.union(b, distinct=kind == "DISTINCT"),  # type: ignore
        [exp.select(*projections).from_(t) for t in tables],
    )


@macro()
def haversine_distance(
    _: MacroEvaluator,
    lat1: exp.Expression,
    lon1: exp.Expression,
    lat2: exp.Expression,
    lon2: exp.Expression,
    unit: exp.Literal = exp.Literal.string("mi"),
) -> exp.Mul:
    """Returns the haversine distance between two points.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @HAVERSINE_DISTANCE(driver_y, driver_x, passenger_y, passenger_x, 'mi') FROM rides"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT 7922 * ASIN(SQRT((POWER(SIN(RADIANS((passenger_y - driver_y) / 2)), 2)) + (COS(RADIANS(driver_y)) * COS(RADIANS(passenger_y)) * POWER(SIN(RADIANS((passenger_x - driver_x) / 2)), 2)))) * 1.0 FROM rides'
    """
    if unit.this == "mi":
        conversion_rate = 1.0
    elif unit.this == "km":
        conversion_rate = 1.60934
    else:
        raise SQLMeshError(f"Invalid unit '{unit}'. Expected 'mi' or 'km'.")

    return (
        2
        * 3961
        * exp.func(
            "ASIN",
            exp.func(
                "SQRT",
                exp.func("POWER", exp.func("SIN", exp.func("RADIANS", (lat2 - lat1) / 2)), 2)
                + exp.func("COS", exp.func("RADIANS", lat1))
                * exp.func("COS", exp.func("RADIANS", lat2))
                * exp.func("POWER", exp.func("SIN", exp.func("RADIANS", (lon2 - lon1) / 2)), 2),
            ),
        )
        * conversion_rate
    )


@macro()
def pivot(
    evaluator: MacroEvaluator,
    column: exp.Column,
    values: t.Union[exp.Array, exp.Tuple],
    alias: exp.Boolean = exp.true(),
    agg: exp.Literal = exp.Literal.string("SUM"),
    cmp: exp.Literal = exp.Literal.string("="),
    prefix: exp.Literal = exp.Literal.string(""),
    suffix: exp.Literal = exp.Literal.string(""),
    then_value: exp.Literal = exp.Literal.number(1),
    else_value: exp.Literal = exp.Literal.number(0),
    quote: exp.Boolean = exp.true(),
    distinct: exp.Boolean = exp.false(),
) -> t.List[exp.Expression]:
    """Returns a list of projections as a result of pivoting the given column on the given values.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT date_day, @PIVOT(status, ['cancelled', 'completed']) FROM rides GROUP BY 1"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT date_day, SUM(CASE WHEN status = \\'cancelled\\' THEN 1 ELSE 0 END) AS "\\'cancelled\\'", SUM(CASE WHEN status = \\'completed\\' THEN 1 ELSE 0 END) AS "\\'completed\\'" FROM rides GROUP BY 1'
    """
    aggregates: t.List[exp.Expression] = []
    for value in values.expressions:
        proj = f"{agg.this}("
        if distinct.this:
            proj += "DISTINCT "
        proj += f"CASE WHEN {column} {cmp.this} {value} THEN {then_value} ELSE {else_value} END) "
        node = evaluator.parse_one(proj)
        if alias.this:
            node = node.as_(f"{prefix.this}{value}{suffix.this}", quoted=quote.this, copy=False)
        aggregates.append(node)
    return aggregates


@macro("AND")
def and_(evaluator: MacroEvaluator, *expressions: t.Optional[exp.Expression]) -> exp.Condition:
    """Returns an AND statement filtering out any NULL expressions."""
    conditions = [e for e in expressions if not isinstance(e, exp.Null)]

    if not conditions:
        return exp.true()

    return exp.and_(*conditions, dialect=evaluator.dialect)


@macro("OR")
def or_(evaluator: MacroEvaluator, *expressions: t.Optional[exp.Expression]) -> exp.Condition:
    """Returns an OR statement filtering out any NULL expressions."""
    conditions = [e for e in expressions if not isinstance(e, exp.Null)]

    if not conditions:
        return exp.true()

    return exp.or_(*conditions, dialect=evaluator.dialect)


@macro("VAR")
def var(
    evaluator: MacroEvaluator, var_name: exp.Expression, default: t.Optional[exp.Expression] = None
) -> exp.Expression:
    """Returns the value of a variable or the default value if the variable is not set."""
    if not var_name.is_string:
        raise SQLMeshError(f"Invalid variable name '{var_name.sql()}'. Expected a string literal.")

    return exp.convert(evaluator.var(var_name.this, default))


@macro()
def deduplicate(
    evaluator: MacroEvaluator,
    relation: exp.Expression,
    partition_by: t.List[exp.Expression],
    order_by: t.List[str],
) -> exp.Query:
    """Returns a QUERY to deduplicate rows within a table

    Args:
        relation: table or CTE name to deduplicate
        partition_by: column names, or expressions to use to identify a window of rows out of which to select one as the deduplicated row
        order_by: A list of strings representing the ORDER BY clause

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@deduplicate(demo.table, [user_id, cast(timestamp as date)], ['timestamp desc', 'status asc'])"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM demo.table QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, CAST(timestamp AS DATE) ORDER BY timestamp DESC, status ASC) = 1'
    """
    if not isinstance(partition_by, list):
        raise SQLMeshError(
            "partition_by must be a list of columns: [<column>, cast(<column> as <type>)]"
        )

    if not isinstance(order_by, list):
        raise SQLMeshError(
            "order_by must be a list of strings, optional - nulls ordering: ['<column> <asc|desc> nulls <first|last>']"
        )

    partition_clause = exp.tuple_(*partition_by)

    order_expressions = [
        evaluator.transform(parse_one(order_item, into=exp.Ordered, dialect=evaluator.dialect))
        for order_item in order_by
    ]

    if not order_expressions:
        raise SQLMeshError(
            "order_by must be a list of strings, optional - nulls ordering: ['<column> <asc|desc> nulls <first|last>']"
        )

    order_clause = exp.Order(expressions=order_expressions)

    window_function = exp.Window(
        this=exp.RowNumber(), partition_by=partition_clause, order=order_clause
    )

    first_unique_row = window_function.eq(1)

    query = exp.select("*").from_(relation).qualify(first_unique_row)

    return query


@macro()
def date_spine(
    evaluator: MacroEvaluator,
    datepart: exp.Expression,
    start_date: exp.Expression,
    end_date: exp.Expression,
) -> exp.Select:
    """Returns a query that produces a date spine with the given datepart, and range of start_date and end_date. Useful for joining as a date lookup table.

    Args:
        datepart: The datepart to use for the date spine - day, week, month, quarter, year
        start_date: The start date for the date spine in format YYYY-MM-DD
        end_date: The end date for the date spine in format YYYY-MM-DD

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@date_spine('week', '2022-01-20', '2024-12-16')"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT date_week FROM UNNEST(GENERATE_DATE_ARRAY(CAST(\'2022-01-20\' AS DATE), CAST(\'2024-12-16\' AS DATE), INTERVAL \'1\' WEEK)) AS _exploded(date_week)"
    """
    datepart_name = datepart.name.lower()
    start_date_name = start_date.name
    end_date_name = end_date.name

    if datepart_name not in ("day", "week", "month", "quarter", "year"):
        raise SQLMeshError(
            f"Invalid datepart '{datepart_name}'. Expected: 'day', 'week', 'month', 'quarter', or 'year'"
        )

    try:
        start_date_obj = datetime.strptime(start_date_name, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date_name, "%Y-%m-%d").date()
    except Exception as e:
        raise SQLMeshError(
            f"Invalid date format - start_date and end_date must be in format: YYYY-MM-DD. Error: {e}"
        )

    if start_date_obj > end_date_obj:
        raise SQLMeshError(
            f"Invalid date range - start_date '{start_date_name}' is after end_date '{end_date_name}'."
        )

    alias_name = f"date_{datepart_name}"
    start_date_column = exp.cast(start_date, "DATE")
    end_date_column = exp.cast(end_date, "DATE")
    if datepart_name == "quarter" and evaluator.dialect in (
        "spark",
        "spark2",
        "databricks",
        "postgres",
    ):
        date_interval = exp.Interval(this=exp.Literal.number(3), unit=exp.var("month"))
    else:
        date_interval = exp.Interval(this=exp.Literal.number(1), unit=exp.var(datepart_name))

    generate_date_array = exp.func(
        "GENERATE_DATE_ARRAY",
        start_date_column,
        end_date_column,
        date_interval,
    )

    exploded = exp.alias_(exp.func("unnest", generate_date_array), "_exploded", table=[alias_name])

    return exp.select(alias_name).from_(exploded)


def normalize_macro_name(name: str) -> str:
    """Prefix macro name with @ and upcase"""
    return f"@{name.upper()}"


for m in macro.get_registry().values():
    setattr(m, c.SQLMESH_BUILTIN, True)
