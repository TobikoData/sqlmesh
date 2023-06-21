from __future__ import annotations

import typing as t
from functools import reduce
from string import Template

import sqlglot
from jinja2 import Environment
from sqlglot import Generator, exp
from sqlglot.executor.env import ENV
from sqlglot.executor.python import Python
from sqlglot.helper import csv, ensure_collection

from sqlmesh.core.dialect import (
    MacroDef,
    MacroFunc,
    MacroSQL,
    MacroStrReplace,
    MacroVar,
)
from sqlmesh.utils import UniqueKeyDict, registry_decorator
from sqlmesh.utils.errors import MacroEvalError, SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, has_jinja
from sqlmesh.utils.metaprogramming import Executable, prepare_env, print_exception


class MacroStrTemplate(Template):
    delimiter = "@"


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
    ):
        self.dialect = dialect
        self.generator = MacroDialect().generator()
        self.locals: t.Dict[str, t.Any] = {}
        self.env = {**ENV, "self": self}
        self.python_env = python_env or {}
        self._jinja_env: t.Optional[Environment] = jinja_env
        self.macros = {normalize_macro_name(k): v.func for k, v in macro.get_registry().items()}
        prepare_env(self.python_env, self.env)
        for k, v in self.python_env.items():
            if v.is_definition:
                self.macros[normalize_macro_name(k)] = self.env[v.name or k]

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
            raise MacroEvalError(f"Error trying to eval macro.") from e

    def transform(self, query: exp.Expression) -> exp.Expression | t.List[exp.Expression] | None:
        changed = False

        def _transform_node(node: exp.Expression) -> exp.Expression:
            nonlocal changed

            if isinstance(node, MacroVar):
                changed = True
                return exp.convert(_norm_env_value(self.locals[node.name]))
            if node.is_string:
                text = node.this
                if has_jinja(text):
                    changed = True
                    node.set("this", self.jinja_env.from_string(node.this).render())
                return node
            return node

        query = query.transform(_transform_node)

        def evaluate_macros(
            node: exp.Expression,
        ) -> exp.Expression | t.List[exp.Expression] | None:
            nonlocal changed

            exp.replace_children(
                node, lambda n: n if isinstance(n, exp.Lambda) else evaluate_macros(n)
            )
            if isinstance(node, MacroFunc):
                changed = True
                return self.evaluate(node)
            return node

        transformed = evaluate_macros(query)

        if changed:
            # the transformations could have corrupted the ast, turning this into sql and reparsing ensures
            # that the ast is correct
            if isinstance(transformed, list):
                return [self.parse_one(node.sql(dialect=self.dialect)) for node in transformed]
            elif isinstance(transformed, exp.Expression):
                return self.parse_one(transformed.sql(dialect=self.dialect))

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
            self.locals[node.name] = node.expression
            return node

        if isinstance(node, (MacroSQL, MacroStrReplace)):
            result: t.Optional[t.Union[exp.Expression | t.List[exp.Expression]]] = exp.convert(
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


class macro(registry_decorator):
    """Specifies a function is a macro and registers it the global MACROS registry.

    Registered macros can be referenced in SQL statements to make queries more dynamic/cleaner.

    Example:
        from typing import t
        from sqlglot import exp
        from sqlmesh.core.macros import MacroEvaluator, macro

        @macro()
        def add_one(evaluator: MacroEvaluator, column: str) -> exp.Add:
            return evaluator.parse_one(f"{column} + 1")

    Args:
        name: A custom name for the macro, the default is the name of the function.
    """

    registry_name = "macros"


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
            if node.name in args and not isinstance(node.parent, exp.Column):
                return args[node.name].copy()
            if "@" in node.name:
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
        return expressions, lambda *args: func.this.transform(
            substitute,
            {expression.name: arg for expression, arg in zip(func.expressions, args)},
        )

    return expressions, func


def _norm_env_value(value: t.Any) -> t.Any:
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, exp.Array):
        return exp.Tuple(expressions=value.expressions)
    return value


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
    return reduce(func, ensure_collection(items))


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


def normalize_macro_name(name: str) -> str:
    """Prefix macro name with @ and upcase"""
    return f"@{name.upper()}"
