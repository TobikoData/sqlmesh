from __future__ import annotations

import importlib
import re
import sys
import traceback
import types
import typing as t
import uuid
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from pathlib import Path

T = t.TypeVar("T")
KEY = t.TypeVar("KEY", bound=t.Hashable)
VALUE = t.TypeVar("VALUE")
DECORATOR_RETURN_TYPE = t.TypeVar("DECORATOR_RETURN_TYPE")


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


def major_minor(version: str) -> t.Tuple[int, int]:
    """Returns a tuple of just the major.minor for a version string (major.minor.patch)."""
    return t.cast(t.Tuple[int, int], tuple(int(part) for part in version.split(".")[0:2]))


def unique(iterable: t.Iterable[T], by: t.Callable[[T], t.Any] = lambda i: i) -> t.List[T]:
    return list({by(i): None for i in iterable})


def random_id() -> str:
    return uuid.uuid4().hex


class UniqueKeyDict(dict, t.Mapping[KEY, VALUE]):
    """Dict that raises when a duplicate key is set."""

    def __init__(self, name: str, *args: t.Dict[KEY, VALUE], **kwargs: VALUE) -> None:
        self.name = name
        super().__init__(*args, **kwargs)

    def __setitem__(self, k: KEY, v: VALUE) -> None:
        if k in self:
            raise ValueError(
                f"Duplicate key '{k}' found in UniqueKeyDict<{self.name}>. Call dict.update(...) if this is intentional."
            )
        super().__setitem__(k, v)


class AttributeDict(dict, t.Mapping[KEY, VALUE]):
    __getattr__ = dict.get

    def __deepcopy__(self, memo: t.Dict[t.Any, AttributeDict]) -> AttributeDict:
        copy: AttributeDict = AttributeDict()
        memo[id(self)] = copy
        for k, v in self.items():
            copy[k] = deepcopy(v, memo)
        return copy


class registry_decorator:
    """A decorator that registers itself."""

    registry_name = ""
    _registry: t.Optional[UniqueKeyDict] = None

    @classmethod
    def registry(cls) -> UniqueKeyDict:
        if cls._registry is None:
            cls._registry = UniqueKeyDict(cls.registry_name)
        return cls._registry

    def __init__(self, name: str = "") -> None:
        self.name = name

    def __call__(
        self, func: t.Callable[..., DECORATOR_RETURN_TYPE]
    ) -> t.Callable[..., DECORATOR_RETURN_TYPE]:
        self.func = func
        self.registry()[(self.name or func.__name__).lower()] = self

        @wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> DECORATOR_RETURN_TYPE:
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
def sys_path(*paths: Path) -> t.Generator[None, None, None]:
    """A context manager to temporarily add a path to 'sys.path'."""
    inserted = set()

    for path in paths:
        path_str = str(path.absolute())

        if path_str not in sys.path:
            sys.path.insert(0, path_str)
            inserted.add(path_str)

    try:
        yield
    finally:
        for path_str in inserted:
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


def double_escape(s: str) -> str:
    """
    Replace backslashes with another backslash.
    """
    return s.replace("\\", "\\\\")


def nullsafe_join(join_char: str, *args: t.Optional[str]) -> str:
    return join_char.join(filter(None, args))


class classproperty(property):
    """
    Similar to a normal property but works for class methods
    """

    def __get__(self, obj: t.Any, owner: t.Any = None) -> t.Any:
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore
