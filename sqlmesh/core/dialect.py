from __future__ import annotations

import functools
import re
import sys
import typing as t
from contextlib import contextmanager
from difflib import unified_diff
from enum import Enum, auto

import pandas as pd
from sqlglot import Dialect, Generator, ParseError, Parser, Tokenizer, TokenType, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.helper import seq_get
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.schema import MappingSchema
from sqlglot.tokens import Token

from sqlmesh.core.constants import MAX_MODEL_DEFINITION_SIZE
from sqlmesh.utils.errors import SQLMeshError, ConfigError
from sqlmesh.utils.pandas import columns_to_types_from_df

if t.TYPE_CHECKING:
    from sqlglot._typing import E


SQLMESH_MACRO_PREFIX = "@"

TABLES_META = "sqlmesh.tables"


class Model(exp.Expression):
    arg_types = {"expressions": True}


class Audit(exp.Expression):
    arg_types = {"expressions": True}


class Metric(exp.Expression):
    arg_types = {"expressions": True}


class Jinja(exp.Func):
    arg_types = {"this": True}


class JinjaQuery(Jinja):
    pass


class JinjaStatement(Jinja):
    pass


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


class MetricAgg(exp.AggFunc):
    """Used for computing metrics."""

    arg_types = {"this": True}

    @property
    def output_name(self) -> str:
        return self.this.name


class StagedFilePath(exp.Table):
    """Represents paths to "staged files" in Snowflake."""


def _parse_statement(self: Parser) -> t.Optional[exp.Expression]:
    if self._curr is None:
        return None

    parser = PARSERS.get(self._curr.text.upper())

    if parser:
        # Capture any available description in the form of a comment
        comments = self._curr.comments

        index = self._index
        try:
            self._advance()
            meta = self._parse_wrapped(lambda: t.cast(t.Callable, parser)(self))
        except ParseError:
            self._retreat(index)

        # Only return the DDL expression if we actually managed to parse one. This is
        # done in order to allow parsing standalone identifiers / function calls like
        # "metric", or "model(1, 2, 3)", which collide with SQLMesh's DDL syntax.
        if self._index != index:
            meta.comments = comments
            return meta

    return self.__parse_statement()  # type: ignore


def _parse_lambda(self: Parser, alias: bool = False) -> t.Optional[exp.Expression]:
    node = self.__parse_lambda(alias=alias)  # type: ignore
    if isinstance(node, exp.Lambda):
        node.set("this", self._parse_alias(node.this))
    return node


def _parse_id_var(
    self: Parser,
    any_token: bool = True,
    tokens: t.Optional[t.Collection[TokenType]] = None,
) -> t.Optional[exp.Expression]:
    if self._prev and self._prev.text == SQLMESH_MACRO_PREFIX and self._match(TokenType.L_BRACE):
        identifier = self.__parse_id_var(any_token=any_token, tokens=tokens)  # type: ignore
        if not self._match(TokenType.R_BRACE):
            self.raise_error("Expecting }")
        identifier.args["this"] = f"@{{{identifier.name}}}"
    else:
        identifier = self.__parse_id_var(any_token=any_token, tokens=tokens)  # type: ignore

    while (
        identifier
        and self._is_connected()
        and (
            self._match_texts(("{", SQLMESH_MACRO_PREFIX))
            or self._curr.token_type not in self.RESERVED_TOKENS
        )
    ):
        this = identifier.name
        brace = False

        if self._prev.text == "{":
            this += "{"
            brace = True
        else:
            if self._prev.text == SQLMESH_MACRO_PREFIX:
                this += "@"
            if self._match(TokenType.L_BRACE):
                this += "{"
                brace = True

        next_id = self._parse_id_var(any_token=False)

        if next_id:
            this += next_id.name
        else:
            return identifier

        if brace:
            if self._match(TokenType.R_BRACE):
                this += "}"
            else:
                self.raise_error("Expecting }")

        identifier = self.expression(exp.Identifier, this=this, quoted=identifier.quoted)

    return identifier


def _parse_macro(self: Parser, keyword_macro: str = "") -> t.Optional[exp.Expression]:
    if self._prev.text != SQLMESH_MACRO_PREFIX:
        return self._parse_parameter()

    comments = self._prev.comments
    index = self._index
    field = self._parse_primary() or self._parse_function(functions={}) or self._parse_id_var()

    def _build_macro(field: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if isinstance(field, exp.Func):
            macro_name = field.name.upper()
            if macro_name != keyword_macro and macro_name in KEYWORD_MACROS:
                self._retreat(index)
                return None

            if isinstance(field, exp.Anonymous):
                if macro_name == "DEF":
                    return self.expression(
                        MacroDef,
                        this=field.expressions[0],
                        expression=field.expressions[1],
                        comments=comments,
                    )
                if macro_name == "SQL":
                    into = field.expressions[1].this.lower() if len(field.expressions) > 1 else None
                    return self.expression(
                        MacroSQL, this=field.expressions[0], into=into, comments=comments
                    )
            else:
                field = self.expression(
                    exp.Anonymous,
                    this=field.sql_name(),
                    expressions=list(field.args.values()),
                    comments=comments,
                )

            return self.expression(MacroFunc, this=field, comments=comments)

        if field is None:
            return None

        if field.is_string or (isinstance(field, exp.Identifier) and field.quoted):
            return self.expression(
                MacroStrReplace, this=exp.Literal.string(field.this), comments=comments
            )

        if "@" in field.this:
            return field
        return self.expression(MacroVar, this=field.this, comments=comments)

    if isinstance(field, (exp.Window, exp.IgnoreNulls, exp.RespectNulls)):
        field.set("this", _build_macro(field.this))
    else:
        field = _build_macro(field)

    return field


KEYWORD_MACROS = {"WITH", "JOIN", "WHERE", "GROUP_BY", "HAVING", "ORDER_BY", "LIMIT"}


def _parse_matching_macro(self: Parser, name: str) -> t.Optional[exp.Expression]:
    if not self._match_pair(TokenType.PARAMETER, TokenType.VAR, advance=False) or (
        self._next and self._next.text.upper() != name.upper()
    ):
        return None

    self._advance()
    return _parse_macro(self, keyword_macro=name)


def _parse_body_macro(self: Parser) -> t.Tuple[str, t.Optional[exp.Expression]]:
    name = self._next and self._next.text.upper()

    if name == "JOIN":
        return ("joins", self._parse_join())
    if name == "WHERE":
        return ("where", self._parse_where())
    if name == "GROUP_BY":
        return ("group", self._parse_group())
    if name == "HAVING":
        return ("having", self._parse_having())
    if name == "ORDER_BY":
        return ("order", self._parse_order())
    if name == "LIMIT":
        return ("limit", self._parse_limit())
    return ("", None)


def _parse_with(self: Parser, skip_with_token: bool = False) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "WITH")
    if not macro:
        return self.__parse_with(skip_with_token=skip_with_token)  # type: ignore

    macro.this.append("expressions", self.__parse_with(skip_with_token=True))  # type: ignore
    return macro


def _parse_join(
    self: Parser, skip_join_token: bool = False, parse_bracket: bool = False
) -> t.Optional[exp.Expression]:
    index = self._index
    method, side, kind = self._parse_join_parts()
    macro = _parse_matching_macro(self, "JOIN")
    if not macro:
        self._retreat(index)
        return self.__parse_join(skip_join_token=skip_join_token, parse_bracket=parse_bracket)  # type: ignore

    join = self.__parse_join(skip_join_token=True)  # type: ignore
    if method:
        join.set("method", method.text)
    if side:
        join.set("side", side.text)
    if kind:
        join.set("kind", kind.text)

    macro.this.append("expressions", join)
    return macro


def _parse_where(self: Parser, skip_where_token: bool = False) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "WHERE")
    if not macro:
        return self.__parse_where(skip_where_token=skip_where_token)  # type: ignore

    macro.this.append("expressions", self.__parse_where(skip_where_token=True))  # type: ignore
    return macro


def _parse_group(self: Parser, skip_group_by_token: bool = False) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "GROUP_BY")
    if not macro:
        return self.__parse_group(skip_group_by_token=skip_group_by_token)  # type: ignore

    macro.this.append("expressions", self.__parse_group(skip_group_by_token=True))  # type: ignore
    return macro


def _parse_having(self: Parser, skip_having_token: bool = False) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "HAVING")
    if not macro:
        return self.__parse_having(skip_having_token=skip_having_token)  # type: ignore

    macro.this.append("expressions", self.__parse_having(skip_having_token=True))  # type: ignore
    return macro


def _parse_order(
    self: Parser, this: t.Optional[exp.Expression] = None, skip_order_token: bool = False
) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "ORDER_BY")
    if not macro:
        return self.__parse_order(this, skip_order_token=skip_order_token)  # type: ignore

    macro.this.append("expressions", self.__parse_order(this, skip_order_token=True))  # type: ignore
    return macro


def _parse_limit(
    self: Parser,
    this: t.Optional[exp.Expression] = None,
    top: bool = False,
    skip_limit_token: bool = False,
) -> t.Optional[exp.Expression]:
    macro = _parse_matching_macro(self, "TOP" if top else "LIMIT")
    if not macro:
        return self.__parse_limit(this, top=top, skip_limit_token=skip_limit_token)  # type: ignore

    macro.this.append("expressions", self.__parse_limit(this, top=top, skip_limit_token=True))  # type: ignore
    return macro


def _parse_props(self: Parser) -> t.Optional[exp.Expression]:
    key = self._parse_id_var(any_token=True)
    if not key:
        return None

    name = key.name.lower()
    if name == "when_matched":
        value: t.Optional[exp.Expression] = self._parse_when_matched()[0]
    elif self._match(TokenType.L_PAREN):
        value = self.expression(exp.Tuple, expressions=self._parse_csv(self._parse_equality))
        self._match_r_paren()
    else:
        value = self._parse_bracket(self._parse_field(any_token=True))

    if name == "path" and value:
        # Make sure if we get a windows path that it is converted to posix
        value = exp.Literal.string(value.this.replace("\\", "/"))

    return self.expression(exp.Property, this=name, value=value)


def _parse_types(
    self: Parser,
    check_func: bool = False,
    schema: bool = False,
    allow_identifiers: bool = True,
) -> t.Optional[exp.Expression]:
    start = self._curr
    parsed_type = self.__parse_types(  # type: ignore
        check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
    )

    if schema and parsed_type:
        parsed_type.meta["sql"] = self._find_sql(start, self._prev)

    return parsed_type


# Only needed for Snowflake: its "staged file" syntax (@<path>) clashes with our macro
# var syntax. By converting the Var representation to a MacroVar, we should be able to
# handle both use cases: if there's no value in the MacroEvaluator's context for that
# MacroVar, it'll render into @<path>, so it won't break staged file path references.
#
# See: https://docs.snowflake.com/en/user-guide/querying-stage
def _parse_table_parts(
    self: Parser, schema: bool = False, is_db_reference: bool = False
) -> exp.Table:
    index = self._index
    table = self.__parse_table_parts(schema=schema, is_db_reference=is_db_reference)  # type: ignore

    table_arg = table.this
    name = table_arg.name

    if isinstance(table_arg, exp.Var) and name.startswith(SQLMESH_MACRO_PREFIX):
        # Macro functions do not clash with the staged file syntax, so we can safely parse them
        from sqlmesh.core.macros import macro

        macros = macro.get_registry()
        if self._prev.token_type == TokenType.STRING or "{" in name or name[1:].lower() in macros:
            self._retreat(index)
            return Parser._parse_table_parts(self, schema=schema, is_db_reference=is_db_reference)

        table_arg.replace(MacroVar(this=name[1:]))
        return StagedFilePath(**table.args)

    return table


def _parse_if(self: Parser) -> t.Optional[exp.Expression]:
    # If we fail to parse an IF function with expressions as arguments, we then try
    # to parse a statement / command to support the macro @IF(condition, statement)
    index = self._index
    try:
        return self.__parse_if()  # type: ignore
    except ParseError:
        self._retreat(index)
        self._match_l_paren()

        cond = self._parse_conjunction()
        self._match(TokenType.COMMA)

        # Try to parse a known statement, otherwise fall back to parsing a command
        # Since the trailing `)` token is not expected by the statement parsers, we
        # remove it from the token stream before trying to parse the statement.
        last_token = self._tokens[-1]
        if last_token.token_type == TokenType.R_PAREN:
            self._tokens[-2].comments.extend(last_token.comments)
            self._tokens.pop()
        else:
            self.raise_error("Expecting )")

        index = self._index
        stmt = self._parse_statement()
        if self._curr:
            self._retreat(index)
            stmt = self._parse_as_command(self._tokens[index])

        return exp.Anonymous(this="IF", expressions=[cond, stmt])


def _create_parser(parser_type: t.Type[exp.Expression], table_keys: t.List[str]) -> t.Callable:
    def parse(self: Parser) -> t.Optional[exp.Expression]:
        from sqlmesh.core.model.kind import ModelKindName

        expressions: t.List[exp.Expression] = []

        while True:
            prev_property = seq_get(expressions, -1)
            if not self._match(TokenType.COMMA, expression=prev_property) and expressions:
                break

            key_expression = self._parse_id_var(any_token=True)
            if not key_expression:
                break

            # This allows macro functions that programmaticaly generate the property key-value pair
            if isinstance(key_expression, MacroFunc):
                expressions.append(key_expression)
                continue

            key = key_expression.name.lower()

            start = self._curr
            value: t.Optional[exp.Expression | str]

            if key in table_keys:
                value = self._parse_table_parts()
            elif key == "columns":
                value = self._parse_schema()
            elif key == "kind":
                id_var = self._parse_id_var(any_token=True)
                if not id_var:
                    value = None
                else:
                    kind = ModelKindName[id_var.name.upper()]

                    if kind in (
                        ModelKindName.INCREMENTAL_BY_TIME_RANGE,
                        ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
                        ModelKindName.INCREMENTAL_BY_PARTITION,
                        ModelKindName.SEED,
                        ModelKindName.VIEW,
                        ModelKindName.SCD_TYPE_2,
                        ModelKindName.SCD_TYPE_2_BY_TIME,
                        ModelKindName.SCD_TYPE_2_BY_COLUMN,
                        ModelKindName.CUSTOM,
                    ) and self._match(TokenType.L_PAREN, advance=False):
                        props = self._parse_wrapped_csv(functools.partial(_parse_props, self))
                    else:
                        props = None

                    value = self.expression(
                        ModelKind,
                        this=kind.value,
                        expressions=props,
                    )
            elif key == "expression":
                value = self._parse_conjunction()
            else:
                value = self._parse_bracket(self._parse_field(any_token=True))

            if isinstance(value, exp.Expression):
                value.meta["sql"] = self._find_sql(start, self._prev)

            expressions.append(self.expression(exp.Property, this=key, value=value))

        return self.expression(parser_type, expressions=expressions)

    return parse


PARSERS = {
    "MODEL": _create_parser(Model, ["name"]),
    "AUDIT": _create_parser(Audit, ["model"]),
    "METRIC": _create_parser(Metric, ["name"]),
}


def _props_sql(self: Generator, expressions: t.List[exp.Expression]) -> str:
    props = []
    size = len(expressions)

    for i, prop in enumerate(expressions):
        sql = self.indent(f"{prop.name} {self.sql(prop, 'value')}")

        if i < size - 1:
            sql += ","
        props.append(self.maybe_comment(sql, expression=prop))

    return "\n".join(props)


def _sqlmesh_ddl_sql(self: Generator, expression: Model | Audit | Metric, name: str) -> str:
    return "\n".join([f"{name} (", _props_sql(self, expression.expressions), ")"])


def _model_kind_sql(self: Generator, expression: ModelKind) -> str:
    props = _props_sql(self, expression.expressions)
    if props:
        return "\n".join([f"{expression.this} (", props, ")"])
    return expression.name.upper()


def _macro_keyword_func_sql(self: Generator, expression: exp.Expression) -> str:
    name = expression.name
    keyword = name.replace("_", " ")
    *args, clause = expression.expressions
    macro = f"@{name}({self.format_args(*args)})"
    return self.sql(clause).replace(keyword, macro, 1)


def _macro_func_sql(self: Generator, expression: MacroFunc) -> str:
    expression = expression.this
    name = expression.name
    if name in KEYWORD_MACROS:
        return _macro_keyword_func_sql(self, expression)
    return f"@{name}({self.format_args(*expression.expressions)})"


def _override(klass: t.Type[Tokenizer | Parser], func: t.Callable) -> None:
    name = func.__name__
    setattr(klass, f"_{name}", getattr(klass, name))
    setattr(klass, name, func)


def format_model_expressions(
    expressions: t.List[exp.Expression], dialect: t.Optional[str] = None, **kwargs: t.Any
) -> str:
    """Format a model's expressions into a standardized format.

    Args:
        expressions: The model's expressions, must be at least model def + query.
        dialect: The dialect to render the expressions as.
        **kwargs: Additional keyword arguments to pass to the sql generator.

    Returns:
        A string representing the formatted model.
    """
    if len(expressions) == 1:
        return expressions[0].sql(pretty=True, dialect=dialect)

    *statements, query = expressions

    def cast_to_colon(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Cast) and not any(
            # Only convert CAST into :: if it doesn't have additional args set, otherwise this
            # conversion could alter the semantics (eg. changing SAFE_CAST in BigQuery to CAST)
            arg
            for name, arg in node.args.items()
            if name not in ("this", "to")
        ):
            this = node.this

            if not isinstance(this, (exp.Binary, exp.Unary)) or isinstance(this, exp.Paren):
                cast = DColonCast(this=this, to=node.to)
                cast.comments = node.comments
                node = cast

        exp.replace_children(node, cast_to_colon)
        return node

    query = query.copy()
    exp.replace_children(query, cast_to_colon)

    return ";\n\n".join(
        [
            *(statement.sql(pretty=True, dialect=dialect, **kwargs) for statement in statements),
            query.sql(pretty=True, dialect=dialect, **kwargs),
        ]
    ).strip()


def text_diff(
    a: t.List[exp.Expression],
    b: t.List[exp.Expression],
    a_dialect: t.Optional[str] = None,
    b_dialect: t.Optional[str] = None,
) -> str:
    """Find the unified text diff between two expressions."""
    a_sql = [
        line
        for expr in a
        for line in expr.sql(pretty=True, comments=False, dialect=a_dialect).split("\n")
    ]
    b_sql = [
        line
        for expr in b
        for line in expr.sql(pretty=True, comments=False, dialect=b_dialect).split("\n")
    ]
    return "\n".join(unified_diff(a_sql, b_sql))


DIALECT_PATTERN = re.compile(
    r"(model|audit).*?\(.*?dialect[^a-z,]+([a-z]*|,)", re.IGNORECASE | re.DOTALL
)


def _is_command_statement(command: str, tokens: t.List[Token], pos: int) -> bool:
    try:
        return (
            tokens[pos].text.upper() == command.upper()
            and tokens[pos + 1].token_type == TokenType.SEMICOLON
        )
    except IndexError:
        return False


JINJA_QUERY_BEGIN = "JINJA_QUERY_BEGIN"
JINJA_STATEMENT_BEGIN = "JINJA_STATEMENT_BEGIN"
JINJA_END = "JINJA_END"


def _is_jinja_statement_begin(tokens: t.List[Token], pos: int) -> bool:
    return _is_command_statement(JINJA_STATEMENT_BEGIN, tokens, pos)


def _is_jinja_query_begin(tokens: t.List[Token], pos: int) -> bool:
    return _is_command_statement(JINJA_QUERY_BEGIN, tokens, pos)


def _is_jinja_end(tokens: t.List[Token], pos: int) -> bool:
    return _is_command_statement(JINJA_END, tokens, pos)


def jinja_query(query: str) -> JinjaQuery:
    return JinjaQuery(this=exp.Literal.string(query.strip()))


def jinja_statement(statement: str) -> JinjaStatement:
    return JinjaStatement(this=exp.Literal.string(statement.strip()))


class ChunkType(Enum):
    JINJA_QUERY = auto()
    JINJA_STATEMENT = auto()
    SQL = auto()


def parse_one(
    sql: str, dialect: t.Optional[str] = None, into: t.Optional[exp.IntoType] = None
) -> exp.Expression:
    expressions = parse(sql, default_dialect=dialect, match_dialect=False, into=into)
    if not expressions:
        raise SQLMeshError(f"No expressions found in '{sql}'")
    elif len(expressions) > 1:
        raise SQLMeshError(f"Multiple expressions found in '{sql}'")
    return expressions[0]


def parse(
    sql: str,
    default_dialect: t.Optional[str] = None,
    match_dialect: bool = True,
    into: t.Optional[exp.IntoType] = None,
) -> t.List[exp.Expression]:
    """Parse a sql string.

    Supports parsing model definition.
    If a jinja block is detected, the query is stored as raw string in a Jinja node.

    Args:
        sql: The sql based definition.
        default_dialect: The dialect to use if the model does not specify one.

    Returns:
        A list of the parsed expressions: [Model, *Statements, Query, *Statements]
    """
    match = match_dialect and DIALECT_PATTERN.search(sql[:MAX_MODEL_DEFINITION_SIZE])
    dialect = Dialect.get_or_raise(match.group(2) if match else default_dialect)

    tokens = dialect.tokenizer.tokenize(sql)
    chunks: t.List[t.Tuple[t.List[Token], ChunkType]] = [([], ChunkType.SQL)]
    total = len(tokens)

    pos = 0
    while pos < total:
        token = tokens[pos]
        if _is_jinja_end(tokens, pos) or (
            chunks[-1][1] == ChunkType.SQL
            and token.token_type == TokenType.SEMICOLON
            and pos < total - 1
        ):
            if token.token_type == TokenType.SEMICOLON:
                pos += 1
            else:
                # Jinja end statement
                chunks[-1][0].append(token)
                pos += 2
            chunks.append(([], ChunkType.SQL))
        elif _is_jinja_query_begin(tokens, pos):
            chunks.append(([token], ChunkType.JINJA_QUERY))
            pos += 2
        elif _is_jinja_statement_begin(tokens, pos):
            chunks.append(([token], ChunkType.JINJA_STATEMENT))
            pos += 2
        else:
            chunks[-1][0].append(token)
            pos += 1

    parser = dialect.parser()
    expressions: t.List[exp.Expression] = []

    for chunk, chunk_type in chunks:
        if chunk_type == ChunkType.SQL:
            parsed_expressions: t.List[t.Optional[exp.Expression]] = (
                parser.parse(chunk, sql) if into is None else parser.parse_into(into, chunk, sql)
            )
            for expression in parsed_expressions:
                if expression:
                    expression.meta["sql"] = parser._find_sql(chunk[0], chunk[-1])
                    expressions.append(expression)
        else:
            start, *_, end = chunk
            segment = sql[start.end + 2 : end.start - 1]
            factory = jinja_query if chunk_type == ChunkType.JINJA_QUERY else jinja_statement
            expression = factory(segment.strip())
            expression.meta["sql"] = sql[start.start : end.end + 1]
            expressions.append(expression)

    return expressions


def extend_sqlglot() -> None:
    """Extend SQLGlot with SQLMesh's custom macro aware dialect."""
    tokenizers = {Tokenizer}
    parsers = {Parser}
    generators = {Generator}

    for dialect in Dialect.classes.values():
        if hasattr(dialect, "Tokenizer"):
            tokenizers.add(dialect.Tokenizer)
        if hasattr(dialect, "Parser"):
            parsers.add(dialect.Parser)
        if hasattr(dialect, "Generator"):
            generators.add(dialect.Generator)

    for tokenizer in tokenizers:
        tokenizer.VAR_SINGLE_TOKENS.update(SQLMESH_MACRO_PREFIX)

    for parser in parsers:
        parser.FUNCTIONS.update({"JINJA": Jinja.from_arg_list, "METRIC": MetricAgg.from_arg_list})
        parser.PLACEHOLDER_PARSERS.update({TokenType.PARAMETER: _parse_macro})
        parser.QUERY_MODIFIER_PARSERS.update(
            {TokenType.PARAMETER: lambda self: _parse_body_macro(self)}
        )

    for generator in generators:
        if MacroFunc not in generator.TRANSFORMS:
            generator.TRANSFORMS.update(
                {
                    Audit: lambda self, e: _sqlmesh_ddl_sql(self, e, "AUDIT"),
                    DColonCast: lambda self, e: f"{self.sql(e, 'this')}::{self.sql(e, 'to')}",
                    Jinja: lambda self, e: e.name,
                    JinjaQuery: lambda self, e: f"{JINJA_QUERY_BEGIN};\n{e.name}\n{JINJA_END};",
                    JinjaStatement: lambda self,
                    e: f"{JINJA_STATEMENT_BEGIN};\n{e.name}\n{JINJA_END};",
                    MacroDef: lambda self, e: f"@DEF({self.sql(e.this)}, {self.sql(e.expression)})",
                    MacroFunc: _macro_func_sql,
                    MacroStrReplace: lambda self, e: f"@{self.sql(e.this)}",
                    MacroSQL: lambda self, e: f"@SQL({self.sql(e.this)})",
                    MacroVar: lambda self, e: f"@{e.name}",
                    Metric: lambda self, e: _sqlmesh_ddl_sql(self, e, "METRIC"),
                    Model: lambda self, e: _sqlmesh_ddl_sql(self, e, "MODEL"),
                    ModelKind: _model_kind_sql,
                    PythonCode: lambda self, e: self.expressions(e, sep="\n", indent=False),
                    StagedFilePath: lambda self, e: self.table_sql(e),
                }
            )

            generator.WITH_SEPARATED_COMMENTS = (
                *generator.WITH_SEPARATED_COMMENTS,
                Model,
                MacroDef,
            )

    _override(Parser, _parse_statement)
    _override(Parser, _parse_join)
    _override(Parser, _parse_order)
    _override(Parser, _parse_where)
    _override(Parser, _parse_group)
    _override(Parser, _parse_with)
    _override(Parser, _parse_having)
    _override(Parser, _parse_limit)
    _override(Parser, _parse_lambda)
    _override(Parser, _parse_types)
    _override(Parser, _parse_if)
    _override(Parser, _parse_id_var)
    _override(Snowflake.Parser, _parse_table_parts)


def select_from_values(
    values: t.List[t.Tuple[t.Any, ...]],
    columns_to_types: t.Dict[str, exp.DataType],
    batch_size: int = 0,
    alias: str = "t",
) -> t.Iterator[exp.Select]:
    """Generate a VALUES expression that has a select wrapped around it to cast the values to their correct types.

    Args:
        values: List of values to use for the VALUES expression.
        columns_to_types: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batches. Defaults to sys.maxsize if <= 0.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    if batch_size <= 0:
        batch_size = sys.maxsize
    num_rows = len(values)
    for i in range(0, num_rows, batch_size):
        yield select_from_values_for_batch_range(
            values=values,
            columns_to_types=columns_to_types,
            batch_start=i,
            batch_end=min(i + batch_size, num_rows),
            alias=alias,
        )


def select_from_values_for_batch_range(
    values: t.List[t.Tuple[t.Any, ...]],
    columns_to_types: t.Dict[str, exp.DataType],
    batch_start: int,
    batch_end: int,
    alias: str = "t",
) -> exp.Select:
    casted_columns = [
        exp.alias_(exp.cast(exp.column(column), to=kind), column, copy=False)
        for column, kind in columns_to_types.items()
    ]

    if not values:
        # Ensures we don't generate an empty VALUES clause & forces a zero-row output
        where = exp.false()
        expressions = [tuple(exp.cast(exp.null(), to=kind) for kind in columns_to_types.values())]
    else:
        where = None
        expressions = [
            tuple(transform_values(v, columns_to_types)) for v in values[batch_start:batch_end]
        ]

    values_exp = exp.values(expressions, alias=alias, columns=columns_to_types)
    if values:
        # BigQuery crashes on `SELECT CAST(x AS TIMESTAMP) FROM UNNEST([NULL]) AS x`, but not
        # on `SELECT CAST(x AS TIMESTAMP) FROM UNNEST([CAST(NULL AS TIMESTAMP)]) AS x`. This
        # ensures nulls under the `Values` expression are cast to avoid similar issues.
        for value, kind in zip(values_exp.expressions[0].expressions, columns_to_types.values()):
            if isinstance(value, exp.Null):
                value.replace(exp.cast(value, to=kind))

    return exp.select(*casted_columns).from_(values_exp, copy=False).where(where, copy=False)


def pandas_to_sql(
    df: pd.DataFrame,
    columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    batch_size: int = 0,
    alias: str = "t",
) -> t.Iterator[exp.Select]:
    """Convert a pandas dataframe into a VALUES sql statement.

    Args:
        df: A pandas dataframe to convert.
        columns_to_types: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batches. Defaults to sys.maxsize if <= 0.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    yield from select_from_values(
        values=list(df.itertuples(index=False, name=None)),
        columns_to_types=columns_to_types or columns_to_types_from_df(df),
        batch_size=batch_size,
        alias=alias,
    )


def set_default_catalog(
    table: str | exp.Table,
    default_catalog: t.Optional[str],
) -> exp.Table:
    table = exp.to_table(table)

    if default_catalog and not table.catalog and table.db:
        table.set("catalog", exp.parse_identifier(default_catalog))

    return table


def normalize_model_name(
    table: str | exp.Table | exp.Column,
    default_catalog: t.Optional[str],
    dialect: DialectType = None,
) -> str:
    if isinstance(table, exp.Column):
        table = exp.table_(table.this, db=table.args.get("table"), catalog=table.args.get("db"))
    else:
        # We are relying on sqlglot's flexible parsing here to accept quotes from other dialects.
        # Ex: I have a a normalized name of '"my_table"' but the dialect is spark and therefore we should
        # expect spark quotes to be backticks ('`') instead of double quotes ('"'). sqlglot today is flexible
        # and will still parse this correctly and we rely on that.
        table = exp.to_table(table, dialect=dialect)

    table = set_default_catalog(table, default_catalog)
    # An alternative way to do this is the following: exp.table_name(table, dialect=dialect, identify=True)
    # This though would result in the names being normalized to the target dialect AND the quotes while the below
    # approach just normalizes the names.
    # By just normalizing names and using sqlglot dialect for quotes this makes it easier for dialects that have
    # compatible normalization strategies but incompatible quoting to still work together without user hassle
    return exp.table_name(normalize_identifiers(table, dialect=dialect), identify=True)


def find_tables(
    expression: exp.Expression, default_catalog: t.Optional[str], dialect: DialectType = None
) -> t.Set[str]:
    """Find all tables referenced in a query.

    Caches the result in the meta field 'tables'.

    Args:
        expressions: The query to find the tables in.
        dialect: The dialect to use for normalization of table names.

    Returns:
        A Set of all the table names.
    """
    if TABLES_META not in expression.meta:
        expression.meta[TABLES_META] = {
            normalize_model_name(table, default_catalog=default_catalog, dialect=dialect)
            for scope in traverse_scope(expression)
            for table in scope.tables
            if table.name and table.name not in scope.cte_sources
        }
    return expression.meta[TABLES_META]


def add_table(node: exp.Expression, table: str) -> exp.Expression:
    """Add a table to all columns in an expression."""

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not node.table:
            return exp.column(node.this, table=table)
        if isinstance(node, exp.Identifier):
            return exp.column(node, table=table)
        return node

    return node.transform(_transform)


def transform_values(
    values: t.Tuple[t.Any, ...], columns_to_types: t.Dict[str, exp.DataType]
) -> t.Iterator[t.Any]:
    """Perform transformations on values given columns_to_types."""
    for value, col_type in zip(values, columns_to_types.values()):
        if col_type.is_type(exp.DataType.Type.JSON):
            yield exp.func("PARSE_JSON", f"'{value}'")
        elif isinstance(value, dict) and col_type.is_type(*exp.DataType.STRUCT_TYPES):
            yield _dict_to_struct(value)
        else:
            yield value


def to_schema(sql_path: str | exp.Table) -> exp.Table:
    if isinstance(sql_path, exp.Table) and sql_path.this is None:
        return sql_path
    table = exp.to_table(sql_path.copy() if isinstance(sql_path, exp.Table) else sql_path)
    table.set("catalog", table.args.get("db"))
    table.set("db", table.args.get("this"))
    table.set("this", None)
    return table


def schema_(
    db: exp.Identifier | str,
    catalog: t.Optional[exp.Identifier | str] = None,
    quoted: t.Optional[bool] = None,
) -> exp.Table:
    """Build a Schema.

    Args:
        db: Database name.
        catalog: Catalog name.
        quoted: Whether to force quotes on the schema's identifiers.

    Returns:
        The new Schema instance.
    """
    return exp.Table(
        this=None,
        db=exp.to_identifier(db, quoted=quoted) if db else None,
        catalog=exp.to_identifier(catalog, quoted=quoted) if catalog else None,
    )


def normalize_mapping_schema(schema: t.Dict, dialect: DialectType) -> MappingSchema:
    return MappingSchema(_unquote_schema(schema), dialect=dialect, normalize=False)


def _unquote_schema(schema: t.Dict) -> t.Dict:
    """SQLGlot schema expects unquoted normalized keys."""
    return {
        k.strip('"'): _unquote_schema(v) if isinstance(v, dict) else v for k, v in schema.items()
    }


def _dict_to_struct(values: t.Dict) -> exp.Struct:
    expressions = []
    for key, value in values.items():
        key = exp.to_identifier(key)
        value = _dict_to_struct(value) if isinstance(value, dict) else exp.convert(value)
        expressions.append(exp.PropertyEQ(this=key, expression=value))

    return exp.Struct(expressions=expressions)


@contextmanager
def normalize_and_quote(
    query: E, dialect: str, default_catalog: t.Optional[str], quote: bool = True
) -> t.Iterator[E]:
    qualify_tables(query, catalog=default_catalog, dialect=dialect)
    normalize_identifiers(query, dialect=dialect)
    yield query
    if quote:
        quote_identifiers(query, dialect=dialect)


def interpret_expression(e: exp.Expression) -> exp.Expression | str | int | float | bool:
    if e.is_int:
        return int(e.this)
    if e.is_number:
        return float(e.this)
    if isinstance(e, (exp.Literal, exp.Boolean)):
        return e.this
    return e


def interpret_key_value_pairs(
    e: exp.Tuple,
) -> t.Dict[str, exp.Expression | str | int | float | bool]:
    return {i.this.name: interpret_expression(i.expression) for i in e.expressions}


def extract_audit(v: exp.Expression) -> t.Tuple[str, t.Dict[str, exp.Expression]]:
    kwargs = {}

    if isinstance(v, exp.Anonymous):
        func = v.name
        args = v.expressions
    elif isinstance(v, exp.Func):
        func = v.sql_name()
        args = list(v.args.values())
    else:
        return v.name.lower(), {}

    for arg in args:
        if not isinstance(arg, (exp.PropertyEQ, exp.EQ)):
            raise ConfigError(
                f"Function '{func}' must be called with key-value arguments like {func}(arg := value)."
            )
        kwargs[arg.left.name.lower()] = arg.right
    return func.lower(), kwargs
