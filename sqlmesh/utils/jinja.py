from __future__ import annotations

import typing as t
from dataclasses import dataclass

from jinja2 import Environment, nodes
from sqlglot import Dialect, Parser, TokenType


def environment(**kwargs: t.Any) -> Environment:
    extensions = kwargs.pop("extensions", [])
    extensions.append("jinja2.ext.do")
    return Environment(extensions=extensions, **kwargs)


ENVIRONMENT = environment()


@dataclass
class MacroInfo:
    """Class to hold macro and its calls"""

    macro: str
    calls: t.List[str]


class MacroExtractor(Parser):
    def extract(self, jinja: str, dialect: str = "") -> t.Dict[str, MacroInfo]:
        """Extract a dictionary of macro definitions from a jinja string.

        Args:
            jinja: The jinja string to extract from.
            dialect: The dialect of SQL.

        Returns:
            A dictionary of macro name to macro definition.
        """
        self.reset()
        self.sql = jinja
        self._tokens = Dialect.get_or_raise(dialect)().tokenizer.tokenize(jinja)
        self._index = -1
        self._advance()

        macros: t.Dict[str, MacroInfo] = {}

        while self._curr:
            if self._at_block_start():
                if self._prev and self._prev.token_type == TokenType.L_BRACE:
                    self._advance()
                macro_start = self._curr
            elif self._tag == "MACRO" and self._next:
                name = self._next.text
                while self._curr and not self._at_block_end():
                    self._advance()
                else:
                    if self._prev and self._prev.token_type == TokenType.R_BRACE:
                        self._advance()

                while self._curr and self._tag != "ENDMACRO":
                    if self._at_block_start():
                        if self._prev and self._prev.token_type == TokenType.L_BRACE:
                            self._advance()

                    self._advance()

                macro_str = self._find_sql(macro_start, self._next)
                macros[name] = MacroInfo(macro=macro_str, calls=extract_call_names(macro_str))

            self._advance()

        return macros

    def _at_block_start(self) -> bool:
        return self._curr.token_type == TokenType.BLOCK_START or self._match_pair(
            TokenType.L_BRACE, TokenType.L_BRACE, advance=False
        )

    def _at_block_end(self) -> bool:
        return self._curr.token_type == TokenType.BLOCK_END or self._match_pair(
            TokenType.R_BRACE, TokenType.R_BRACE, advance=False
        )

    def _advance(self, times: int = 1) -> None:
        super()._advance(times)
        self._tag = (
            self._curr.text.upper()
            if self._curr
            and self._prev
            and (
                self._prev.token_type == TokenType.BLOCK_START
                or (
                    self._index > 1
                    and self._tokens[self._index - 1].token_type == TokenType.L_BRACE
                    and self._tokens[self._index - 2].token_type == TokenType.L_BRACE
                )
            )
            else ""
        )


def call_name(node: nodes.Expr) -> str:
    if isinstance(node, nodes.Name):
        return node.name
    if isinstance(node, nodes.Const):
        return f"'{node.value}'"
    if isinstance(node, nodes.Getattr):
        return f"{call_name(node.node)}.{node.attr}"
    if isinstance(node, (nodes.Getitem, nodes.Call)):
        return call_name(node.node)
    return ""


def find_call_names(node: nodes.Node, vars_in_scope: t.Set[str]) -> t.Generator[str, None, None]:
    vars_in_scope = vars_in_scope.copy()
    for child_node in node.iter_child_nodes():
        if "target" in child_node.fields:
            target = getattr(child_node, "target")
            if isinstance(target, nodes.Name):
                vars_in_scope.add(target.name)
            elif isinstance(target, nodes.Tuple):
                for item in target.items:
                    if isinstance(item, nodes.Name):
                        vars_in_scope.add(item.name)
        elif isinstance(child_node, nodes.Macro):
            for arg in child_node.args:
                vars_in_scope.add(arg.name)
        elif isinstance(child_node, nodes.Call):
            name = call_name(child_node)
            if name[0] != "'" and not name.split(".")[0] in vars_in_scope:
                yield name
        yield from find_call_names(child_node, vars_in_scope)


def extract_call_names(jinja_str: str) -> t.List[str]:
    return list(find_call_names(ENVIRONMENT.parse(jinja_str), set()))
