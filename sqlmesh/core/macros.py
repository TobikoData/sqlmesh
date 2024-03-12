from __future__ import annotations

import inspect
import logging
import typing as t
from enum import Enum
from functools import reduce, wraps
from string import Template

import sqlglot
from jinja2 import Environment
from sqlglot import Generator, exp, parse_one
from sqlglot.executor.env import ENV
from sqlglot.executor.python import Python
from sqlglot.helper import csv, ensure_collection
from sqlglot.schema import MappingSchema

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
from sqlmesh.utils import DECORATOR_RETURN_TYPE, UniqueKeyDict, registry_decorator
from sqlmesh.utils.errors import MacroEvalError, SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, has_jinja
from sqlmesh.utils.metaprogramming import Executable, prepare_env, print_exception

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.snapshot import Snapshot

logger = logging.getLogger(__name__)


class RuntimeStage(Enum):
    LOADING = "loading"
    CREATING = "creating"
    EVALUATING = "evaluating"


class MacroStrTemplate(Template):
    delimiter = SQLMESH_MACRO_PREFIX


EXPRESSIONS_NAME_MAP = {}

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
        dialect: str = "",
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        jinja_env: t.Optional[Environment] = None,
        schema: t.Optional[MappingSchema] = None,
        runtime_stage: RuntimeStage = RuntimeStage.LOADING,
        resolve_tables: t.Optional[t.Callable[[exp.Expression], exp.Expression]] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        default_catalog: t.Optional[str] = None,
    ):
        self.dialect = dialect
        self.generator = MacroDialect().generator()
        self.locals: t.Dict[str, t.Any] = {
            "runtime_stage": runtime_stage.value,
            "default_catalog": default_catalog,
        }
        self.env = {**ENV, "self": self}
        self.python_env = python_env or {}
        self._jinja_env: t.Optional[Environment] = jinja_env
        self.macros = {normalize_macro_name(k): v.func for k, v in macro.get_registry().items()}
        self._schema = schema
        self._resolve_tables = resolve_tables
        self.columns_to_types_called = False
        self._snapshots = snapshots if snapshots is not None else {}
        self.default_catalog = default_catalog

        prepare_env(self.python_env, self.env)
        for k, v in self.python_env.items():
            if v.is_definition:
                self.macros[normalize_macro_name(k)] = self.env[v.name or k]
            elif v.is_import and getattr(self.env.get(k), "__sqlmesh_macro__", None):
                self.macros[normalize_macro_name(k)] = self.env[k]
            elif v.is_value:
                self.locals[k] = self.env[k]

    def send(
        self, name: str, *args: t.Any
    ) -> t.Union[None, exp.Expression, t.List[exp.Expression]]:
        func = self.macros.get(normalize_macro_name(name))

        if not callable(func):
            raise SQLMeshError(f"Macro '{name}' does not exist.")

        try:
            return func(self, *args)
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

            exp.replace_children(
                node, lambda n: n if isinstance(n, exp.Lambda) else evaluate_macros(n)
            )
            if isinstance(node, MacroVar):
                changed = True
                if node.name not in self.locals:
                    if not isinstance(node.parent, StagedFilePath):
                        raise SQLMeshError(f"Macro variable '{node.name}' is undefined.")

                    return node

                value = self.locals[node.name]
                return exp.convert(tuple(value) if isinstance(value, list) else value)
            if isinstance(node, exp.Identifier):
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

        transformed = evaluate_macros(expression.copy())

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
        return MacroStrTemplate(str(text)).safe_substitute(self.locals, **local_variables)

    def evaluate(self, node: MacroFunc) -> exp.Expression | t.List[exp.Expression] | None:
        if isinstance(node, MacroDef):
            if isinstance(node.expression, exp.Lambda):
                _, fn = _norm_var_arg_lambda(self, node.expression)
                self.macros[normalize_macro_name(node.name)] = lambda _, *args: fn(
                    list(args) if len(args) <= 1 else exp.Tuple(expressions=list(args))
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
            result = self.send(func.name, *func.expressions)

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
        if self._schema is None or self._schema.empty:
            self.columns_to_types_called = True
            return {"__schema_unavailable_at_load__": exp.DataType.build("unknown")}

        if isinstance(model_name, exp.Column):
            model_name = exp.table_(
                model_name.this,
                db=model_name.args.get("table"),
                catalog=model_name.args.get("db"),
            )

        columns_to_types = self._schema.find(exp.to_table(model_name))
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

    def _coerce(self, expr: exp.Expression, typ: t.Any, strict: bool = False) -> t.Any:
        """Coerces the given expression to the specified type on a best-effort basis."""
        base_err_msg = f"Failed to coerce expression '{expr}' to type '{typ}'."
        try:
            if typ is None or typ is t.Any:
                return expr
            base = t.get_origin(typ) or typ
            # We need to handle t.Union first since we cannot use isinstance with it
            if base is t.Union:
                for branch in t.get_args(typ):
                    try:
                        return self._coerce(expr, branch, True)
                    except Exception:
                        pass
                raise SQLMeshError(base_err_msg)
            if isinstance(expr, base):
                return expr
            if issubclass(base, exp.Expression):
                d = Dialect.get_or_raise(self.dialect)
                into = base if base in d.parser().EXPRESSION_PARSERS else None
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
            if base is bool and isinstance(expr, exp.Boolean):
                return expr.this
            if base is str and isinstance(expr, exp.Expression):
                return expr.sql(self.dialect)
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
            logger.warning(
                "Coercion of expression '%s' to type '%s' failed. Using non coerced expression.",
                expr,
                typ,
                exc_info=True,
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

    def __call__(
        self, func: t.Callable[..., DECORATOR_RETURN_TYPE]
    ) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
        @wraps(func)
        def _typed_func(
            evaluator: MacroEvaluator, *args_: t.Any, **kwargs_: t.Any
        ) -> DECORATOR_RETURN_TYPE:
            spec = inspect.getfullargspec(func)
            annotations = t.get_type_hints(func)
            kwargs = inspect.getcallargs(func, evaluator, *args_, **kwargs_)
            for param, value in kwargs.items():
                coercible_type = annotations.get(param)
                if not coercible_type:
                    continue
                kwargs[param] = evaluator._coerce(value, coercible_type)
            args = [kwargs.pop(k) for k in spec.args if k in kwargs]
            if spec.varargs:
                args.extend(kwargs.pop(spec.varargs, []))
            return func(*args, **kwargs)

        try:
            annotated = any(t.get_type_hints(func).keys() - {"return"})
        except TypeError:
            annotated = False

        wrapper = super().__call__(
            func if not annotated else t.cast(t.Callable[..., DECORATOR_RETURN_TYPE], _typed_func)
        )

        # This is useful to identify macros at runtime
        setattr(wrapper, "__sqlmesh_macro__", True)
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


@macro("WITH")
def with_(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.With,
) -> t.Optional[exp.With]:
    """Inserts WITH expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@WITH(True) all_cities as (select * from city) select all_cities"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'WITH all_cities AS (SELECT * FROM city) SELECT all_cities'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: With expression
    Returns:
        With expression if the conditional is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


@macro()
def join(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Join,
) -> t.Optional[exp.Join]:
    """Inserts JOIN expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "select * from city @JOIN(True) country on city.country = country.name"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city JOIN country ON city.country = country.name'

        >>> sql = "select * from city left outer @JOIN(True) country on city.country = country.name"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city LEFT OUTER JOIN country ON city.country = country.name'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: Join expression
    Returns:
        Join expression if the conditional is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


@macro()
def where(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Where,
) -> t.Optional[exp.Where]:
    """Inserts WHERE expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "select * from city @WHERE(True) population > 100 and country = 'Mexico'"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT * FROM city WHERE population > 100 AND country = 'Mexico'"

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: Where expression
    Returns:
        Where expression if condition is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


@macro()
def group_by(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Group,
) -> t.Optional[exp.Group]:
    """Inserts GROUP BY expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator, group_by
        >>> sql = "select * from city @GROUP_BY(True) country, population"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city GROUP BY country, population'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: Group expression
    Returns:
        Group expression if the condition is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


@macro()
def having(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Having,
) -> t.Optional[exp.Having]:
    """Inserts HAVING expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "select * from city group by country @HAVING(True) population > 100 and country = 'Mexico'"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT * FROM city GROUP BY country HAVING population > 100 AND country = 'Mexico'"

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: Having expression
    Returns:
        Having expression if the condition is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


@macro()
def order_by(
    evaluator: MacroEvaluator,
    condition: exp.Condition,
    expression: exp.Order,
) -> t.Optional[exp.Order]:
    """Inserts ORDER BY expression when the condition is True

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "select * from city @ORDER_BY(True) population, name DESC"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM city ORDER BY population, name DESC'

    Args:
        evaluator: MacroEvaluator that invoked the macro
        condition: Condition expression
        expression: Order expression
    Returns:
        Order expression if the condition is True; otherwise None
    """
    return expression if evaluator.eval_expression(condition) else None


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


@macro()
def star(
    evaluator: MacroEvaluator,
    relation: exp.Table,
    alias: exp.Column = t.cast(exp.Column, exp.column("")),
    except_: exp.Array | exp.Tuple = exp.Tuple(this=[]),
    prefix: exp.Literal = exp.Literal.string(""),
    suffix: exp.Literal = exp.Literal.string(""),
    quote_identifiers: exp.Boolean = exp.true(),
) -> t.List[exp.Alias]:
    """Returns a list of projections for the given relation.

    Args:
        evaluator: MacroEvaluator that invoked the macro
        relation: The relation to select star from
        alias: The alias of the relation
        except_: Columns to exclude
        prefix: A prefix to use for all selections
        suffix: A suffix to use for all selections
        quote_identifiers: Whether or not quote the resulting aliases, defaults to true
    Returns:
        An array of columns.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @STAR(foo, bar, [c], 'baz_') FROM foo AS bar"
        >>> MacroEvaluator(schema=MappingSchema({"foo": {"a": "string", "b": "string", "c": "string", "d": "int"}})).transform(parse_one(sql)).sql()
        'SELECT CAST("bar"."a" AS TEXT) AS "baz_a", CAST("bar"."b" AS TEXT) AS "baz_b", CAST("bar"."d" AS INT) AS "baz_d" FROM foo AS bar'
    """
    if alias and not isinstance(alias, (exp.Identifier, exp.Column)):
        raise SQLMeshError(f"Invalid alias '{alias}'. Expected an identifier.")
    if except_ and not isinstance(except_, (exp.Array, exp.Tuple)):
        raise SQLMeshError(f"Invalid except '{except_}'. Expected an array.")
    if prefix and not isinstance(prefix, exp.Literal):
        raise SQLMeshError(f"Invalid prefix '{prefix}'. Expected a literal.")
    if suffix and not isinstance(suffix, exp.Literal):
        raise SQLMeshError(f"Invalid suffix '{suffix}'. Expected a literal.")
    if not isinstance(quote_identifiers, exp.Boolean):
        raise SQLMeshError(f"Invalid quote_identifiers '{quote_identifiers}'. Expected a boolean.")

    exclude = {e.name for e in except_.expressions}
    quoted = quote_identifiers.this

    return [
        exp.cast(exp.column(column, table=alias.name, quoted=quoted), type_).as_(
            f"{prefix.this}{column}{suffix.this}", quoted=quoted
        )
        for column, type_ in evaluator.columns_to_types(relation).items()
        if column not in exclude
    ]


@macro()
def generate_surrogate_key(_: MacroEvaluator, *fields: exp.Column) -> exp.Func:
    """Generates a surrogate key for the given fields.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @GENERATE_SURROGATE_KEY(a, b, c) FROM foo"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT MD5(CONCAT(COALESCE(CAST(a AS TEXT), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(b AS TEXT), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(c AS TEXT), '_sqlmesh_surrogate_key_null_'))) FROM foo"
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
    return exp.func("MD5", exp.func("CONCAT", *string_fields))


@macro()
def safe_add(_: MacroEvaluator, *fields: exp.Column) -> exp.Case:
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
    *tables: exp.Column,  # These represent tables but the ast node will be columns
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
        exp.cast(column, type_).as_(column)
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
    values: exp.Array | exp.Tuple,
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
        "SELECT date_day, SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) FROM rides GROUP BY 1"
    """
    aggregates: t.List[exp.Expression] = []
    for value in values.expressions:
        proj = f"{agg.this}("
        if distinct.this:
            proj += "DISTINCT "
        proj += f"CASE WHEN {column} {cmp.this} {value} THEN {then_value} ELSE {else_value} END) "
        node = evaluator.parse_one(proj)
        if alias.this:
            node.as_(f"{prefix.this}{value}{suffix.this}", quoted=quote.this, copy=False)
        aggregates.append(node)
    return aggregates


def normalize_macro_name(name: str) -> str:
    """Prefix macro name with @ and upcase"""
    return f"@{name.upper()}"
