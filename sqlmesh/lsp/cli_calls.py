from pathlib import Path
import pathlib
import os
import time
import errno
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh._version import __version__
import typing as t
from pydantic import Field


class DaemonCommunicationModeTCP(PydanticModel):
    type: t.Literal["tcp"] = "tcp"
    address: str


class DaemonCommunicationModeUnixSocket(PydanticModel):
    type: t.Literal["unix_socket"] = "unix_socket"
    socket: str


class DaemonCommunicationMode(PydanticModel):
    type: t.Union[DaemonCommunicationModeTCP, DaemonCommunicationModeUnixSocket] = Field(
        discriminator="type"
    )


class LockFile(PydanticModel):
    version: str
    file_path: str
    communication: t.Optional[DaemonCommunicationMode] = None

    def validate_lock_file(self, other: "LockFile") -> bool:
        return self.version == other.version and self.file_path == other.file_path


def generate_lock_file() -> LockFile:
    version = __version__
    bin_path = pathlib.Path(__file__).resolve()
    return LockFile(
        version=version,
        file_path=str(bin_path),
    )


def return_lock_file_path(project_path: Path) -> Path:
    return project_path / ".sqlmesh" / ".lsp_lock"


class FileLock:
    """
    A simple, portable file‑based lock.

    Usage:
        with FileLock("/tmp/mylock.lock"):
            # critical section
            ...
    """

    def __init__(self, lock_path: str, timeout: float = 10.0, wait: float = 0.1):
        """
        :param lock_path: Path to the lock file.
        :param timeout:  Maximum seconds to wait before raising TimeoutError.
        :param wait:    Sleep time (in seconds) between attempts.
        """
        self.lock_path = lock_path
        self.timeout = timeout
        self.wait = wait
        self.fd = None  # file descriptor returned by os.open

    def acquire(self) -> None:
        """
        Acquire the lock. Blocks until the lock is obtained or the timeout
        is reached, in which case a TimeoutError is raised.
        """
        start = time.monotonic()
        while True:
            try:
                # O_CREAT | O_EXCL guarantees atomic creation.
                self.fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                    0o644,  # permissions for the new file
                )
                # Optional: write the PID into the file so that stale locks can
                # be identified later.
                os.write(self.fd, str(os.getpid()).encode())
                return
            except FileExistsError:
                # File already exists – somebody else holds the lock.
                if time.monotonic() - start > self.timeout:
                    raise TimeoutError(
                        f"Could not acquire lock on {self.lock_path} after {self.timeout} seconds"
                    )
                time.sleep(self.wait)
            except OSError as exc:
                # A different OSError – re‑raise so callers can see it.
                raise exc

    def release(self) -> None:
        """
        Release the lock and delete the lock file.
        """
        if self.fd is not None:
            try:
                os.close(self.fd)
            finally:
                self.fd = None

        # Delete the file only if we created it. If another process created a
        # file with the same name while we were running we must not delete it.
        try:
            os.unlink(self.lock_path)
        except FileNotFoundError:
            # The file might have been removed by another process.
            pass
        except PermissionError as exc:
            # On Windows a file can’t be removed if it is still opened by
            # another process; swallow the error – the lock is already
            # released because we closed our fd.
            if exc.errno != errno.EACCES:
                raise

    # Context‑manager support ---------------------------------------------
    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()
