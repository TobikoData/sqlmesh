import json
import random
import time
import typing as t
from contextlib import contextmanager
from uuid import uuid4

import fsspec

class FileTransactionHandler:
    """A transaction handler which wraps a file path."""

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

    @property
    def lock_path(self) -> str:
        """The key which represents a path to the file's lock."""
        return f"{self.path}.lock"

    def read(self) -> t.Optional[bytes]:
        """Reads data from the file."""
        if self._fs.exists(self.path):
            if not self._is_locked:
                # TODO: do whatever we think is right here
                # if a file is read without a lock, we should probably
                # denote it as a read-only transaction
                ... 
            content = self._fs.cat(self.path)
            self._original_contents = content.copy()
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

    def acquire_lock(
        self, blocking: bool = True, timeout: float = -1
    ) -> bool:
        """Acquires a lock on a key.
        
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
        lock_id = uuid4()
        wait_time = 0
        while self._fs.exists(self.lock_path):
            if not blocking:
                return False
            time.sleep(1)
            wait_time += 1
            if timeout > 0 and wait_time > timeout:
                return False
        self._fs.pipe(self.lock_path, lock_id.bytes)
        self._jitter()
        lock_content = self._fs.cat(self.lock_path)
        acquired = lock_content == lock_id.bytes
        if not acquired:
            # This indicates contention from a concurrent process.
            # Retrying will now result in a wait.
            return self.acquire_lock(blocking, timeout)
        self._is_locked = True
        return True

    def release_lock(self) -> None:
        """Releases a lock on a key."""
        if self._is_locked:
            self._fs.rm(self.lock_path)
            self._is_locked = False
            self._original_contents = None

    @contextmanager
    def read_with_lock(self, timeout: float = 30.0) -> t.Iterator[t.Optional[bytes]]:
        """A context manager that acquires and releases a lock on a path."""
        if not self.acquire_lock(timeout=timeout):
            raise RuntimeError("Could not acquire lock.")
        try:
            yield self.read()
        except Exception:
            self.rollback()
            raise
        finally:
            self.release_lock()

    @staticmethod
    def _jitter() -> None:
        """Introduces a small amount of jitter to avoid contention."""
        jitter = random.random() * 0.1
        time.sleep(0.1 + jitter)
