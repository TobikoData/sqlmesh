from __future__ import annotations

import copy
import importlib
import os
import random
import re
import string
import sys
import time
import traceback
import types
import typing as t
import uuid
from contextlib import contextmanager
from copy import deepcopy
from functools import lru_cache, reduce, wraps
from pathlib import Path

from sqlglot.dialects.dialect import Dialects

T = t.TypeVar("T")
KEY = t.TypeVar("KEY", bound=t.Hashable)
VALUE = t.TypeVar("VALUE")
DECORATOR_RETURN_TYPE = t.TypeVar("DECORATOR_RETURN_TYPE")

ALPHANUMERIC = string.ascii_lowercase + string.digits


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


def random_id(short: bool = False) -> str:
    if short:
        return "".join(random.choices(ALPHANUMERIC, k=8))

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

    def set(self, field: str, value: t.Any) -> str:
        self[field] = value
        # Return an empty string, so that this method can be used within Jinja
        return ""

    def __deepcopy__(self, memo: t.Dict[t.Any, AttributeDict]) -> AttributeDict:
        copy: AttributeDict = AttributeDict()
        memo[id(self)] = copy
        for k, v in self.items():
            copy[k] = deepcopy(v, memo)
        return copy

    def __call__(self, **kwargs: t.Dict[str, t.Any]) -> str:
        self.update(**kwargs)
        # Return an empty string, so that this method can be used within Jinja
        return ""


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
def sys_path(*paths: Path) -> t.Iterator[None]:
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


def str_to_bool(s: t.Optional[str]) -> bool:
    """
    Convert a string to a boolean. disutils is being deprecated and it is recommended to implement your own version:
    https://peps.python.org/pep-0632/

    Unlike disutils, this actually returns a bool and never raises. If a value cannot be determined to be true
    then false is returned.
    """
    if not s:
        return False
    return s.lower() in ("true", "1", "t", "y", "yes", "on")


_debug_mode_enabled: bool = False


def enable_debug_mode() -> None:
    global _debug_mode_enabled
    _debug_mode_enabled = True


def debug_mode_enabled() -> bool:
    return _debug_mode_enabled or str_to_bool(os.environ.get("SQLMESH_DEBUG"))


def ttl_cache(ttl: int = 60, maxsize: int = 128000) -> t.Callable:
    """Caches a function that clears whenever the current epoch / ttl seconds changes.

    TTL is not exact, it is used as a salt. So by default, at every minute mark, the cache will be cleared.
    This is done for simplicity.

    Args:
        ttl: The number of seconds to hold the cache for.
        maxsize: The maximum size of the cache.
    """

    def decorator(func: t.Callable) -> t.Any:
        @lru_cache(maxsize=maxsize)
        def cache(tick: int, *args: t.Any, **kwargs: t.Any) -> t.Any:
            return func(*args, **kwargs)

        @wraps(func)
        def wrap(*args: t.Any, **kwargs: t.Any) -> t.Any:
            return cache(int(time.time() / ttl), *args, **kwargs)

        return wrap

    return decorator


class classproperty(property):
    """
    Similar to a normal property but works for class methods
    """

    def __get__(self, obj: t.Any, owner: t.Any = None) -> t.Any:
        return classmethod(self.fget).__get__(None, owner)()  # type: ignore


@contextmanager
def env_vars(environ: dict[str, str]) -> t.Iterator[None]:
    """A context manager to temporarily modify environment variables."""
    old_environ = os.environ.copy()
    os.environ.update(environ)

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def merge_dicts(*args: t.Dict) -> t.Dict:
    """
    Merges dicts. Just does key collision replacement
    """

    def merge(a: t.Dict, b: t.Dict) -> t.Dict:
        for b_key, b_value in b.items():
            a_value = a.get(b_key)
            if isinstance(a_value, dict) and isinstance(b_value, dict):
                merge(a_value, b_value)
            elif isinstance(b_value, dict):
                a[b_key] = copy.deepcopy(b_value)
            else:
                a[b_key] = b_value
        return a

    return reduce(merge, args, {})


def sqlglot_dialects() -> str:
    return "'" + "', '".join(Dialects.__members__.values()) + "'"


NON_ALNUM = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_name(name: str) -> str:
    return NON_ALNUM.sub("_", name)
