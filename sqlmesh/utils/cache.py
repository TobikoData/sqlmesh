from __future__ import annotations

import gzip
import logging
import pickle
import typing as t
from pathlib import Path

from sqlglot import __version__ as SQLGLOT_VERSION

from sqlmesh.utils import sanitize_name
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

logger = logging.getLogger(__name__)

T = t.TypeVar("T", bound=PydanticModel)


SQLGLOT_VERSION_TUPLE = tuple(SQLGLOT_VERSION.split("."))
SQLGLOT_MAJOR_VERSION = SQLGLOT_VERSION_TUPLE[0]
SQLGLOT_MINOR_VERSION = SQLGLOT_VERSION_TUPLE[1]


class FileCache(t.Generic[T]):
    """Generic file-based cache implementation.

    Args:
        path: The path to the cache folder.
        entry_class: The type of cached entries.
        prefix: The prefix shared between all entries to distinguish them from other entries
            stored in the same cache folder.
    """

    def __init__(
        self,
        path: Path,
        entry_class: t.Type[T],
        prefix: t.Optional[str] = None,
    ):
        self._path = path / prefix if prefix else path
        self._entry_class = entry_class

    def get_or_load(self, name: str, entry_id: str, loader: t.Callable[[], T]) -> T:
        """Returns an existing cached entry or loads and caches a new one.

        Args:
            name: The name of the entry.
            entry_id: The unique entry identifier. Used for cache invalidation.
            loader: Used to load a new entry when no cached instance was found.

        Returns:
            The entry.
        """
        cached_entry = self.get(name, entry_id)
        if cached_entry:
            return cached_entry

        loaded_entry = loader()
        self.put(name, entry_id, loaded_entry)
        return loaded_entry

    def get(self, name: str, entry_id: str) -> t.Optional[T]:
        """Returns a cached entry if exists.

        Args:
            name: The name of the entry.
            entry_id: The unique entry identifier. Used for cache invalidation.

        Returns:
            The entry or None if no entry was found in the cache.
        """
        cache_entry_path = self._cache_entry_path(name, entry_id)
        if cache_entry_path.exists():
            with gzip.open(cache_entry_path, "rb") as fd:
                try:
                    return self._entry_class.parse_obj(pickle.load(fd))
                except Exception as ex:
                    logger.warning("Failed to load a cache entry '%s': %s", name, ex)

        return None

    def put(self, name: str, entry_id: str, value: T) -> None:
        """Stores the given value in the cache.

        Args:
            name: The name of the entry.
            entry_id: The unique entry identifier. Used for cache invalidation.
            value: The value to store in the cache.
        """
        if not self._path.exists():
            self._path.mkdir(parents=True)
        if not self._path.is_dir():
            raise SQLMeshError(f"Cache path '{self._path}' is not a directory.")

        for obsolete_cache_file in self._path.glob(f"{name}__*"):
            obsolete_cache_file.unlink()

        with gzip.open(self._cache_entry_path(name, entry_id), "wb", compresslevel=1) as fd:
            pickle.dump(value.dict(), fd)

    def _cache_entry_path(self, name: str, entry_id: str) -> Path:
        from sqlmesh.core.state_sync.base import SCHEMA_VERSION

        try:
            from sqlmesh._version import __version_tuple__

            major, minor = __version_tuple__[0], __version_tuple__[1]
        except ImportError:
            major, minor = 0, 0
        entry_file_name = "__".join(
            [
                name,
                str(major),
                str(minor),
                SQLGLOT_MAJOR_VERSION,
                SQLGLOT_MINOR_VERSION,
                str(SCHEMA_VERSION),
                entry_id,
            ]
        )
        return self._path / sanitize_name(entry_file_name)
