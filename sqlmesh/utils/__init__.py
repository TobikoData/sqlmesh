import importlib
import os
import types
import typing as t
import uuid
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
