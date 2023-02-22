from __future__ import annotations

import typing as t
from dataclasses import dataclass

from jinja2 import Environment, Undefined, nodes
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

                body_start = self._next

                while self._curr and self._tag != "ENDMACRO":
                    if self._at_block_start():
                        body_end = self._prev
                        if self._prev and self._prev.token_type == TokenType.L_BRACE:
                            self._advance()

                    self._advance()

                macro_str = self._find_sql(macro_start, self._next)
                macros[name] = MacroInfo(macro=macro_str, calls=find_call_names(macro_str))

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


def call_name(node: nodes.Name | nodes.Getattr | nodes.Call) -> str:
    if isinstance(node, nodes.Name):
        return node.name
    if isinstance(node, nodes.Getattr):
        return f"{call_name(node.node)}.{node.attr}"
    return call_name(node.node)


def find_call_names(node: node.Node) -> t.List[str]:
    """Find all call names in a Jinja node."""
	return [call_name(call) for call in node.find_all(nodes.Call)]


#class Placeholder(str):
#    def __call__(self, *args: t.Any, **kwargs: t.Any) -> str:
#        return ""
#
#
#@dataclass
#class CapturedQuery:
#    """Helper class to hold a rendered jinja query and all function calls."""
#
#    query: str
#    calls: t.List[t.Tuple[str, t.Tuple[t.Any, ...], t.Dict[str, t.Any]]]
#
#
#def capture_jinja(query: str) -> CapturedQuery:
#    """
#    Render the jinja in the provided string using the passed in environment
#
#    Args:
#        query: The string to render
#
#    Returns:
#        The jinja rendered string
#    """
#    calls = []
#
#    class UndefinedSpy(Undefined):
#        def _fail_with_undefined_error(self, *args: t.Any, **kwargs: t.Any):  # type: ignore
#            calls.append((self._undefined_name, args, kwargs))
#            return Placeholder()
#
#        __add__ = __radd__ = __sub__ = __rsub__ = _fail_with_undefined_error
#        __mul__ = __rmul__ = __div__ = __rdiv__ = _fail_with_undefined_error
#        __truediv__ = __rtruediv__ = _fail_with_undefined_error
#        __floordiv__ = __rfloordiv__ = _fail_with_undefined_error
#        __mod__ = __rmod__ = _fail_with_undefined_error
#        __pos__ = __neg__ = _fail_with_undefined_error
#        __call__ = __getitem__ = _fail_with_undefined_error
#        __lt__ = __le__ = __gt__ = __ge__ = _fail_with_undefined_error
#        __int__ = __float__ = __complex__ = _fail_with_undefined_error
#        __pow__ = __rpow__ = _fail_with_undefined_error
#
#    return CapturedQuery(
#        query=environment(undefined=UndefinedSpy).from_string(query).render(),
#        calls=calls,  # type: ignore
#    )
