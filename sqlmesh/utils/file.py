import os
import time
import typing as t
from contextlib import contextmanager
from datetime import datetime, timedelta
from uuid import uuid4

import fsspec

POLLING_INTERVAL = 1.0


class FileTransactionHandler:
    """A transaction handler which wraps a file path."""

    mtime_dispatch = {
        "s3": lambda f: datetime.strptime(f["LastModified"], "%Y-%m-%d %H:%M:%S%z"),
        "gcs": lambda f: datetime.strptime(f["updated"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        "azure": lambda f: datetime.strptime(f["LastModified"], "%Y-%m-%d %H:%M:%S%z"),
        "file": lambda f: datetime.fromtimestamp(f["mtime"]),
    }

    def __init__(self, path: str) -> None:
        """Creates a new FileTransactionHandler.

        Args:
            path: The path to lock. It should follow the format of a table name.
                Something like snapshots/version=<ver>/<snapshot_id>
        """
        self.path = path
        # TODO: pass in a filesystem, naturally
        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem("file")
        self._original_contents: t.Optional[bytes] = None
        self._is_locked = False
        self._lock_id = uuid4()
        self._lock_timeout = timedelta(minutes=1)

    @property
    def lock_prefix(self) -> str:
        """The prefix for the lock file."""
        return f"{self.path}.lock."

    @property
    def lock_path(self) -> str:
        """The path to the lock file."""
        return self.lock_prefix + str(self._lock_id)

    def _get_base_lock_path(self) -> str:
        """Gets the base lock path for a list command."""
        if self._fs.protocol == "file":
            return os.path.abspath(os.path.dirname(self.lock_path))
        return self.lock_prefix

    def _get_locks(self) -> t.Dict[str, datetime]:
        """Gets a map of lock paths to their last modified times."""
        output = {}
        for lock in (
            lock_info
            for lock_info in self._fs.ls(
                self._get_base_lock_path(), refresh=True, detail=True
            )
            if os.path.basename(lock_info["name"]).startswith(
                os.path.basename(self.lock_prefix)
            )
        ):
            mtime = self.mtime_dispatch[self._fs.protocol](lock)
            if datetime.now() - mtime > self._lock_timeout:
                # Handle stale locks
                self._fs.rm(lock["name"])
                print(f"Removed stale lock {lock['name']}")
                continue
            output[os.path.basename(lock["name"])] = mtime
        return output

    def read(self) -> t.Optional[bytes]:
        """Reads data from the file."""
        if self._fs.exists(self.path):
            content: bytes = self._fs.cat(self.path)
            self._original_contents = content[:]  # copy the bytes
            return content
        return None

    def write(self, content: bytes) -> None:
        """Writes data within the transaction."""
        if not self._is_locked:
            raise RuntimeError("Cannot write to a file without a lock.")
        self._fs.pipe(self.path, content)

    def rollback(self) -> None:
        """Rolls back the transaction."""
        if self._original_contents is not None:
            self._fs.pipe(self.path, self._original_contents)

    def acquire_lock(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquires a lock on a path.

        Acquire a lock, blocking or non-blocking.

        When invoked with the blocking argument set to True (the default), block until
        the lock is unlocked, then set it to locked and return True.

        When invoked with the blocking argument set to False, do not block. If a call
        with blocking set to True would block, return False immediately; otherwise, set
        the lock to locked and return True.

        When invoked with the floating-point timeout argument set to a positive value,
        block for at most the number of seconds specified by timeout and as long as the
        lock cannot be acquired. A timeout argument of -1 specifies an unbounded wait.
        If blocking is False, timeout is ignored. The stdlib would raise a ValueError.

        The return value is True if the lock is acquired successfully, False if
        not (for example if the timeout expired).
        """
        if self._is_locked:
            # Make this idempotent for a single instance.
            return True
        wait_time = 0.0
        self._fs.touch(self.lock_path)
        locks = self._get_locks()
        active_lock = min(locks, key=locks.get)
        while active_lock != os.path.basename(self.lock_path):
            if not blocking:
                return False
            if timeout > 0 and wait_time > timeout:
                return False
            time.sleep(POLLING_INTERVAL)
            wait_time += POLLING_INTERVAL
            locks = self._get_locks()
            active_lock = min(locks, key=locks.get)
        print(f"Acquired lock {self.lock_path} after {wait_time} seconds.")
        self._is_locked = True
        return True

    def release_lock(self) -> None:
        """Releases a lock on a key.

        This is idempotent and safe to call multiple times.
        """
        if self._is_locked:
            self._fs.rm(self.lock_path)
            self._is_locked = False
            self._original_contents = None

    @contextmanager
    def read_with_lock(self, timeout: float = 61.0) -> t.Iterator[t.Optional[bytes]]:
        """A context manager that acquires and releases a lock on a path.

        This is a convenience method for acquiring a lock and reading the contents of
        the file. It will release the lock when the context manager exits. This is
        useful for reading a file and then writing it back out as a transaction. If
        the lock cannot be acquired, this will raise a RuntimeError. If the lock is
        acquired, the contents of the file will be returned. If the file does not
        exist, None will be returned. If an exception is raised within the context
        manager, the transaction will be rolled back.

        Args:
            timeout: The timeout for acquiring the lock.

        Raises:
            RuntimeError: If the lock cannot be acquired.
        """
        if not self.acquire_lock(timeout=timeout):
            raise RuntimeError("Could not acquire lock.")
        try:
            yield self.read()
        except Exception:
            self.rollback()
            raise
        finally:
            self.release_lock()
