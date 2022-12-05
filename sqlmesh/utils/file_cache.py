from __future__ import annotations

import pickle
import typing as t
from collections.abc import MutableMapping
from pathlib import Path


class FileCache(MutableMapping):
    """A simple file persisted dictionary.

    FileCache uses pickle to serialize itself.

    FileCache syncs to disk on init and when sync() is called. It can also be used in a context like

    with FileCache(path) as cache:
        cache["x"] = 1

    which first syncs the cache with store and then syncs to disk on exit.

    Args:
        file: The file path to store the data.
    """

    def __init__(self, file: Path, *args, **kwargs):
        self.file = file
        self.store: t.Dict[t.Any, t.Any] = {}
        self.update(dict(*args, **kwargs))
        self.sync()

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __enter__(self) -> FileCache:
        with open(self.file, "rb") as f:
            self._load(f)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.sync()

    def _load(self, f) -> None:
        for k, v in pickle.load(f).items():
            if k not in self.store:
                self.store[k] = v

    def clear(self) -> None:
        """Clear the store and save an empty cache file."""
        self.store.clear()
        self.sync(True)

    def sync(self, overwrite=False) -> None:
        """Load the file if it exists and add new keys that don't exist in memory and then overwrite the file.

        Args:
            overwrite: If set to true, always write a new file with the contents of this store without merging.
        """
        if not overwrite and self.file.is_file():
            with open(self.file, "rb+") as f:
                self._load(f)
                f.seek(0)
                pickle.dump(self.store, f, protocol=4)
                f.truncate()
        else:
            with open(self.file, "wb") as f:
                pickle.dump(self.store, f, protocol=4)
