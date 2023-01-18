from __future__ import annotations

import typing as t
from dataclasses import dataclass

from jinja2 import Environment, Undefined


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
