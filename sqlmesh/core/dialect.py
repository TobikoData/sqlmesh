from __future__ import annotations

import functools
import re
import typing as t
from difflib import unified_diff

import pandas as pd
from jinja2.meta import find_undeclared_variables
from sqlglot import Dialect, Generator, Parser, TokenType, exp

from sqlmesh.utils.jinja import ENVIRONMENT


class Model(exp.Expression):
    arg_types = {"expressions": True}


class Audit(exp.Expression):
    arg_types = {"expressions": True}


class Jinja(exp.Func):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ModelKind(exp.Expression):
    arg_types = {"this": True, "expressions": False}


class MacroVar(exp.Var):
    pass


class MacroFunc(exp.Func):
    @property
    def name(self) -> str:
        return self.this.name


class MacroDef(MacroFunc):
    arg_types = {"this": True, "expression": True}


class MacroSQL(MacroFunc):
    arg_types = {"this": True, "into": False}


class MacroStrReplace(MacroFunc):
    pass


class PythonCode(exp.Expression):
    arg_types = {"expressions": True}


class DColonCast(exp.Cast):
    pass


@t.no_type_check
def _parse_statement(self: Parser) -> t.Optional[exp.Expression]:
    if self._curr is None:
        return None

    parser = PARSERS.get(self._curr.text.upper())

    if parser:
        # Capture any available description in the form of a comment
        comments = self._curr.comments

        self._advance()
        meta = self._parse_wrapped(lambda: parser(self))

        meta.comments = comments
        return meta
    return self.__parse_statement()


@t.no_type_check
def _parse_lambda(self: Parser) -> t.Optional[exp.Expression]:
    node = self.__parse_lambda()
    if isinstance(node, exp.Lambda):
        node.set("this", self._parse_alias(node.this))
    return node


def _parse_macro(self: Parser, keyword_macro: str = "") -> t.Optional[exp.Expression]:
    index = self._index
    field = self._parse_primary() or self._parse_function({}) or self._parse_id_var()

    if isinstance(field, exp.Func):
        macro_name = field.name.upper()
        if macro_name != keyword_macro and macro_name in KEYWORD_MACROS:
            self._retreat(index)
            return None
        if isinstance(field, exp.Anonymous):
            name = field.name.upper()
            if name == "DEF":
                return self.expression(
                    MacroDef, this=field.expressions[0], expression=field.expressions[1]
                )
            if name == "SQL":
                into = field.expressions[1].this.lower() if len(field.expressions) > 1 else None
                return self.expression(MacroSQL, this=field.expressions[0], into=into)
        return self.expression(MacroFunc, this=field)

    if field is None:
        return None

    if field.is_string or (isinstance(field, exp.Identifier) and field.quoted):
        return self.expression(MacroStrReplace, this=exp.Literal.string(field.this))
    return self.expression(MacroVar, this=field.this)


KEYWORD_MACROS = {"WITH", "JOIN", "WHERE", "GROUP_BY", "HAVING", "ORDER_BY"}


def _parse_matching_macro(self: Parser, name: str) -> t.Optional[exp.Expression]:
    if not self._match_pair(TokenType.PARAMETER, TokenType.VAR, advance=False) or (
        self._next and self._next.text.upper() != name.upper()
    ):
        return None

    self._advance(1)
    return _parse_macro(self, keyword_macro=name)


@t.no_type_check
def _parse_with(self: Parser) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "WITH")
    if not macro:
        return self.__parse_with()

    macro.this.append("expressions", self.__parse_with(True))
    return macro


@t.no_type_check
def _parse_join(self: Parser) -> t.Optional[exp.Expression]:
    index = self._index
    natural, side, kind = self._parse_join_side_and_kind()
    macro = _parse_matching_macro(self, "JOIN")
    if not macro:
        self._retreat(index)
        return self.__parse_join()

    join = self.__parse_join(True)
    if natural:
        join.set("natural", True)
    if side:
        join.set("side", side.text)
    if kind:
        join.set("kind", kind.text)

    macro.this.append("expressions", join)
    return macro


@t.no_type_check
def _parse_where(self: Parser) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "WHERE")
    if not macro:
        return self.__parse_where()

    macro.this.append("expressions", self.__parse_where(True))
    return macro


@t.no_type_check
def _parse_group(self: Parser) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "GROUP_BY")
    if not macro:
        return self.__parse_group()

    macro.this.append("expressions", self.__parse_group(True))
    return macro


@t.no_type_check
def _parse_having(self: Parser) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "HAVING")
    if not macro:
        return self.__parse_having()

    macro.this.append("expressions", self.__parse_having(True))
    return macro


@t.no_type_check
def _parse_order(self: Parser, this: exp.Expression = None) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "ORDER_BY")
    if not macro:
        return self.__parse_order(this)

    macro.this.append("expressions", self.__parse_order(this, True))
    return macro


def _parse_props(self: Parser) -> t.Optional[exp.Expression]:
    key = self._parse_id_var(True)

    if not key:
        return None

    index = self._index
    if self._match(TokenType.L_PAREN):
        self._retreat(index)
        value: t.Optional[exp.Expression] = self.expression(
            exp.Tuple,
            expressions=self._parse_wrapped_csv(
                lambda: self._parse_string() or self._parse_id_var()
            ),
        )
    else:
        value = self._parse_bracket(self._parse_field(any_token=True))

    return self.expression(exp.Property, this=key.name.lower(), value=value)


def _create_parser(parser_type: t.Type[exp.Expression], table_keys: t.List[str]) -> t.Callable:
    def parse(self: Parser) -> t.Optional[exp.Expression]:
        from sqlmesh.core.model.kind import ModelKindName

        expressions = []

        while True:
            key_expression = self._parse_id_var(any_token=True)

            if not key_expression:
                break

            key = key_expression.name.lower()

            value: t.Optional[exp.Expression | str]

            if key in table_keys:
                value = exp.table_name(self._parse_table())
            elif key == "columns":
                value = self._parse_schema()
            elif key == "kind":
                id_var = self._parse_id_var(any_token=True)
                if not id_var:
                    value = None
                else:
                    index = self._index
                    kind = ModelKindName[id_var.name.upper()]

                    if kind in (
                        ModelKindName.INCREMENTAL_BY_TIME_RANGE,
                        ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
                        ModelKindName.SEED,
                    ) and self._match(TokenType.L_PAREN):
                        self._retreat(index)
                        props = self._parse_wrapped_csv(functools.partial(_parse_props, self))
                    else:
                        props = None
                    value = self.expression(
                        ModelKind,
                        this=kind.value,
                        expressions=props,
                    )
            else:
                value = self._parse_bracket(self._parse_field(any_token=True))

            expressions.append(self.expression(exp.Property, this=key, value=value))

            if not self._match(TokenType.COMMA):
                break

        return self.expression(parser_type, expressions=expressions)

    return parse


_parse_model = _create_parser(Model, ["name"])
_parse_audit = _create_parser(Audit, ["model"])
PARSERS = {"MODEL": _parse_model, "AUDIT": _parse_audit}


def _model_sql(self: Generator, expression: exp.Expression) -> str:
    props = ",\n".join(
        self.indent(f"{prop.name} {self.sql(prop, 'value')}") for prop in expression.expressions
    )
    return "\n".join(["MODEL (", props, ")"])


def _model_kind_sql(self: Generator, expression: ModelKind) -> str:
    props = ",\n".join(
        self.indent(f"{prop.this} {self.sql(prop, 'value')}") for prop in expression.expressions
    )
    if props:
        return "\n".join([f"{expression.this} (", props, ")"])
    return expression.name.upper()


def _macro_keyword_func_sql(self: Generator, expression: exp.Expression) -> str:
    name = expression.name
    keyword = name.replace("_", " ")
    *args, clause = expression.expressions
    macro = f"@{name}({self.format_args(*args)})"
    return self.sql(clause).replace(keyword, macro, 1)


def _macro_func_sql(self: Generator, expression: exp.Expression) -> str:
    expression = expression.this
    name = expression.name
    if name in KEYWORD_MACROS:
        return _macro_keyword_func_sql(self, expression)
    return f"@{name}({self.format_args(*expression.expressions)})"


def _override(klass: t.Type[Parser], func: t.Callable) -> None:
    name = func.__name__
    setattr(klass, f"_{name}", getattr(klass, name))
    setattr(klass, name, func)


def format_model_expressions(
    expressions: t.List[exp.Expression], dialect: t.Optional[str] = None
) -> str:
    """Format a model's expressions into a standardized format.

    Args:
        expressions: The model's expressions, must be at least model def + query.
        dialect: The dialect to render the expressions as.
    Returns:
        A string with the formatted model.
    """
    if len(expressions) == 1:
        return expressions[0].sql(pretty=True, dialect=dialect)

    *statements, query = expressions
    query = query.copy()
    selects = []

    for expression in query.expressions:
        column = None
        comments = expression.comments
        expression.comments = None

        if not isinstance(expression, exp.Alias):
            if expression.name:
                expression = expression.replace(exp.alias_(expression.copy(), expression.name))

        column = column or expression
        expression = expression.this

        if isinstance(expression, exp.Cast):
            this = expression.this
            if not isinstance(this, (exp.Binary, exp.Unary)) or isinstance(this, exp.Paren):
                expression.replace(DColonCast(this=this, to=expression.to))
        column.comments = comments
        selects.append(column)

    query.set("expressions", selects)

    return ";\n\n".join(
        [
            *(statement.sql(pretty=True, dialect=dialect) for statement in statements),
            query.sql(pretty=True, dialect=dialect),
        ]
    ).strip()


def text_diff(
    a: t.Optional[exp.Expression],
    b: t.Optional[exp.Expression],
    dialect: t.Optional[str] = None,
) -> str:
    """Find the unified text diff between two expressions."""
    return "\n".join(
        unified_diff(
            a.sql(pretty=True, comments=False, dialect=dialect).split("\n") if a else "",
            b.sql(pretty=True, comments=False, dialect=dialect).split("\n") if b else "",
        )
    )


DIALECT_PATTERN = re.compile(
    r"(model|audit).*?\(.*?dialect[^a-z,]+([a-z]*|,)", re.IGNORECASE | re.DOTALL
)


def parse(sql: str, default_dialect: str | None = None) -> t.List[exp.Expression]:
    """Parse a sql string.

    Supports parsing model definition.
    If a jinja block is detected, the query is stored as raw string in a Jinja node.

    Args:
        sql: The sql based definition.
        default_dialect: The dialect to use if the model does not specify one.

    Returns:
        A list of the expressions, [Model, *Statements, Query | Jinja]
    """
    match = DIALECT_PATTERN.search(sql)
    dialect = Dialect.get_or_raise(match.group(2) if match else default_dialect)()

    tokens = dialect.tokenizer.tokenize(sql)
    chunks: t.List[t.Tuple[t.List, bool]] = [([], False)]
    total = len(tokens)

    for i, token in enumerate(tokens):
        if token.token_type == TokenType.SEMICOLON:
            if i < total - 1:
                chunks.append(([], False))
        else:
            if token.token_type == TokenType.BLOCK_START or (
                i < total - 1
                and token.token_type == TokenType.L_BRACE
                and tokens[i + 1].token_type == TokenType.L_BRACE
            ):
                chunks[-1] = (chunks[-1][0], True)
            chunks[-1][0].append(token)

    expressions: t.List[exp.Expression] = []
    sql_lines = None

    for chunk, is_jinja in chunks:
        if is_jinja:
            start, *_, end = chunk
            sql_lines = sql_lines or sql.split("\n")
            lines = sql_lines[start.line - 1 : end.line]
            lines[0] = lines[0][start.col - 1 :]
            lines[-1] = lines[-1][: end.col + len(end.text) - 1]
            segment = "\n".join(lines)
            variables = [
                exp.Literal.string(var)
                for var in find_undeclared_variables(ENVIRONMENT.parse(segment))
            ]
            expressions.append(Jinja(this=exp.Literal.string(segment), expressions=variables))
        else:
            for expression in dialect.parser().parse(chunk, sql):
                if expression:
                    expressions.append(expression)

    return expressions


@t.no_type_check
def extend_sqlglot() -> None:
    """Extend SQLGlot with SQLMesh's custom macro aware dialect."""
    parsers = {Parser}
    generators = {Generator}

    for dialect in Dialect.classes.values():
        if hasattr(dialect, "Parser"):
            parsers.add(dialect.Parser)
        if hasattr(dialect, "Generator"):
            generators.add(dialect.Generator)

    for generator in generators:
        if MacroFunc not in generator.TRANSFORMS:
            generator.TRANSFORMS.update(
                {
                    DColonCast: lambda self, e: f"{self.sql(e, 'this')}::{self.sql(e, 'to')}",
                    MacroDef: lambda self, e: f"@DEF({self.sql(e.this)}, {self.sql(e.expression)})",
                    MacroFunc: _macro_func_sql,
                    MacroStrReplace: lambda self, e: f"@{self.sql(e.this)}",
                    MacroSQL: lambda self, e: f"@SQL({self.sql(e.this)})",
                    MacroVar: lambda self, e: f"@{e.name}",
                    Model: _model_sql,
                    Jinja: lambda self, e: e.name,
                    ModelKind: _model_kind_sql,
                    PythonCode: lambda self, e: self.expressions(e, sep="\n", indent=False),
                }
            )
            generator.WITH_SEPARATED_COMMENTS = (
                *generator.WITH_SEPARATED_COMMENTS,
                Model,
            )

    for parser in parsers:
        parser.FUNCTIONS.update(
            {
                "JINJA": Jinja.from_arg_list,
            }
        )
        parser.PLACEHOLDER_PARSERS.update(
            {
                TokenType.PARAMETER: _parse_macro,
            }
        )

    _override(Parser, _parse_statement)
    _override(Parser, _parse_join)
    _override(Parser, _parse_order)
    _override(Parser, _parse_where)
    _override(Parser, _parse_group)
    _override(Parser, _parse_with)
    _override(Parser, _parse_having)
    _override(Parser, _parse_lambda)


def select_from_values(
    values: t.Iterable[t.Tuple[t.Any, ...]],
    columns_to_types: t.Dict[str, exp.DataType],
    batch_size: int = 0,
    alias: str = "t",
) -> t.Generator[exp.Select, None, None]:
    """Generate a VALUES expression that has a select wrapped around it to cast the values to their correct types.

    Args:
        values: List of values to use for the VALUES expression.
        columns_to_types: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batch, if <= 0 then no batching will occur.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    casted_columns = [
        exp.alias_(exp.cast(column, to=kind), column) for column, kind in columns_to_types.items()
    ]
    batch = []
    for row in values:
        batch.append(row)
        if batch_size > 0 and len(batch) > batch_size:
            values_exp = exp.values(batch, alias=alias, columns=columns_to_types)
            yield exp.select(*casted_columns).from_(values_exp)
            batch.clear()
    if batch:
        values_exp = exp.values(batch, alias=alias, columns=columns_to_types)
        yield exp.select(*casted_columns).from_(values_exp)


def pandas_to_sql(
    df: pd.DataFrame,
    columns_to_types: t.Dict[str, exp.DataType],
    batch_size: int = 0,
    alias: str = "t",
) -> t.Generator[exp.Select, None, None]:
    """Convert a pandas dataframe into a VALUES sql statement.

    Args:
        df: A pandas dataframe to convert.
        columns_to_types: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batch, if <= 0 then no batching will occur.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    yield from select_from_values(
        values=df.itertuples(index=False, name=None),
        columns_to_types=columns_to_types,
        batch_size=batch_size,
        alias=alias,
    )
