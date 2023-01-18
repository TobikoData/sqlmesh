from __future__ import annotations

import pickle
import typing as t
from pathlib import Path
from types import TracebackType

KEY = t.TypeVar("KEY", bound=t.Hashable)
VALUE = t.TypeVar("VALUE")


class FileCache(t.MutableMapping[KEY, VALUE]):
    """A simple file persisted dictionary.

    FileCache uses pickle to serialize itself.

    FileCache syncs to disk on init and when sync() is called. It can also be used in a context like

    with FileCache(path) as cache:
        cache["x"] = 1

    which first syncs the cache with store and then syncs to disk on exit.

    Args:
        file: The file path to store the data.
    """

    def __init__(self, file: Path, *args: t.Dict[KEY, VALUE], **kwargs: VALUE) -> None:
        self.file = file
        self.store: t.Dict[KEY, VALUE] = {}
        self.update(*args, **kwargs)
        self.sync()

    def __getitem__(self, key: KEY) -> VALUE:
        return self.store[key]

    def __setitem__(self, key: KEY, value: VALUE) -> None:
        self.store[key] = value

    def __delitem__(self, key: KEY) -> None:
        del self.store[key]

    def __iter__(self) -> t.Iterator[KEY]:
        return iter(self.store)

    def __len__(self) -> int:
        return len(self.store)

    def __enter__(self) -> FileCache:
        with open(self.file, "rb") as f:
            self._load(f)
        return self

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_value: t.Optional[BaseException],
        exc_traceback: t.Optional[TracebackType],
    ) -> None:
        self.sync()

    def _load(self, f: t.BinaryIO) -> None:
        for k, v in pickle.load(f).items():
            if k not in self.store:
                self.store[k] = v

    def clear(self) -> None:
        """Clear the store and save an empty cache file."""
        self.store.clear()
        self.sync(True)

    def sync(self, overwrite: bool = False) -> None:
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
