from __future__ import annotations

import typing as t
from dataclasses import dataclass

from jinja2 import Environment, Undefined
from sqlglot import Dialect, Parser, TokenType


@dataclass
class MacroInfo:
    """Class to hold macro and its calls"""

    macro: str
    calls: t.List[t.Tuple[str, t.Tuple[t.Any, ...], t.Dict[str, t.Any]]]


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
        self.sql = jinja or ""
        self._tokens = Dialect.get_or_raise(dialect)().tokenizer.tokenize(jinja)
        self._index = -1
        self._advance()

        macros: t.Dict[str, MacroInfo] = {}

        while self._curr:
            if self._curr.token_type == TokenType.BLOCK_START:
                macro_start = self._curr
            elif self._tag == "MACRO" and self._next:
                name = self._next.text
                while self._curr and self._curr.token_type != TokenType.BLOCK_END:
                    self._advance()
                body_start = self._next

                while self._curr and self._tag != "ENDMACRO":
                    if self._curr.token_type == TokenType.BLOCK_START:
                        body_end = self._prev
                    self._advance()

                calls = capture_jinja(self._find_sql(body_start, body_end)).calls
                macros[name] = MacroInfo(
                    macro=self._find_sql(macro_start, self._next), calls=calls
                )

            self._advance()

        return macros

    def _advance(self, times: int = 1) -> None:
        super()._advance(times)
        self._tag = (
            self._curr.text.upper()
            if self._curr
            and self._prev
            and self._prev.token_type == TokenType.BLOCK_START
            else ""
        )


class Placeholder(str):
    def __call__(self, *args: t.Any, **kwargs: t.Any) -> str:
        return ""


@dataclass
class CapturedQuery:
    """Helper class to hold a rendered jinja query and all function calls."""

    query: str
    calls: t.List[t.Tuple[str, t.Tuple[t.Any, ...], t.Dict[str, t.Any]]]


def capture_jinja(query: str) -> CapturedQuery:
    """
    Render the jinja in the provided string using the passed in environment

    Args:
        query: The string to render
        env: jinja methods to use during rendering

    Returns:
        The jinja rendered string
    """
    calls = []

    class UndefinedSpy(Undefined):
        def _fail_with_undefined_error(  # type: ignore
            self, *args: t.Any, **kwargs: t.Any
        ):
            calls.append((self._undefined_name, args, kwargs))
            return Placeholder()

        __add__ = __radd__ = __sub__ = __rsub__ = _fail_with_undefined_error
        __mul__ = __rmul__ = __div__ = __rdiv__ = _fail_with_undefined_error
        __truediv__ = __rtruediv__ = _fail_with_undefined_error
        __floordiv__ = __rfloordiv__ = _fail_with_undefined_error
        __mod__ = __rmod__ = _fail_with_undefined_error
        __pos__ = __neg__ = _fail_with_undefined_error
        __call__ = __getitem__ = _fail_with_undefined_error
        __lt__ = __le__ = __gt__ = __ge__ = _fail_with_undefined_error
        __int__ = __float__ = __complex__ = _fail_with_undefined_error
        __pow__ = __rpow__ = _fail_with_undefined_error

    return CapturedQuery(
        query=Environment(undefined=UndefinedSpy).from_string(query).render(),
        calls=calls,  # type: ignore
    )
