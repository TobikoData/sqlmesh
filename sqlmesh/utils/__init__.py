from __future__ import annotations

import importlib
import re
import sys
import traceback
import types
import typing as t
import uuid
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

T = t.TypeVar("T")


def optional_import(name: str) -> t.Optional[types.ModuleType]:
    """Optionally import a module.

    Args:
        name: The name of the module to import.
    Returns:
        The module if it is installed.
    """
    try:
        module = importlib.import_module(name)
    except ImportError:
        return None
    return module


def unique(
    iterable: t.Iterable[T], by: t.Callable[[T], t.Any] = lambda i: i
) -> t.List[T]:
    return list({by(i): None for i in iterable})


def random_id() -> str:
    return str(uuid.uuid4()).replace("-", "_")


class UniqueKeyDict(dict):
    """Dict that raises when a duplicate key is set."""

    def __init__(self, name: str, *args, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)

    def __setitem__(self, k, v):
        if k in self:
            raise ValueError(
                f"Duplicate key '{k}' found in UniqueKeyDict<{self.name}>. Call dict.update(...) if this is intentional."
            )
        super().__setitem__(k, v)


class registry_decorator:
    """A decorator that registers itself."""

    registry_name = ""
    _registry: t.Optional[UniqueKeyDict] = None

    @classmethod
    def registry(cls):
        if cls._registry is None:
            cls._registry = UniqueKeyDict(cls.registry_name)
        return cls._registry

    def __init__(self, name: str = ""):
        self.name = name

    def __call__(self, func: t.Callable) -> t.Callable:
        self.func = func
        self.registry()[(self.name or func.__name__)] = self

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    @classmethod
    def get_registry(cls) -> UniqueKeyDict:
        """Get a copy of the registry"""
        return UniqueKeyDict(cls.registry_name, **(cls._registry or {}))

    @classmethod
    def set_registry(cls, registry: UniqueKeyDict) -> None:
        """Set the registry."""
        cls._registry = registry


@contextmanager
def sys_path(path: Path):
    """A context manager to temporarily add a path to 'sys.path'."""
    path_str = str(path.absolute())

    if path_str in sys.path:
        inserted = False
    else:
        sys.path.insert(0, path_str)
        inserted = True

    try:
        yield
    finally:
        if inserted:
            sys.path.remove(path_str)


def format_exception(exception: BaseException) -> t.List[str]:
    if sys.version_info < (3, 10):
        return traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )  # type: ignore
    else:
        return traceback.format_exception(exception)  # type: ignore


def word_characters_only(s: str, replacement_char: str = "_") -> str:
    """
    Replace all non-word characters in string with the replacement character.
    Reference SO: https://stackoverflow.com/questions/1276764/stripping-everything-but-alphanumeric-chars-from-a-string-in-python/70310018#70310018

    >>> word_characters_only("Hello, world!")
    'Hello__world_'
    >>> word_characters_only("Hello, world! 123", '')
    'Helloworld123'
    """
    return re.sub(r"\W", replacement_char, s)
