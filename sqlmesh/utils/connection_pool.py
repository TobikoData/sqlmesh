import abc
import logging
import typing as t
from threading import Lock, get_ident

logger = logging.getLogger(__name__)


class ConnectionPool(abc.ABC):
    def get_cursor(self) -> t.Any:
        """Returns cached cursor instance.

        Automatically creates a new instance if one is not available.

        Returns:
            A cursor instance.
        """

    def get(self) -> t.Any:
        """Returns cached connection instance.

        Automatically opens a new connection if one is not available.

        Returns:
            A connection instance.
        """

    def close_cursor(self) -> None:
        """Closes the current cursor instance if exists."""

    def close(self) -> None:
        """Closes the current connection instance if exists.

        Note: if there is a cursor instance available it will be closed as well.
        """

    def close_all(self, exclude_calling_thread: bool = False) -> None:
        """Closes all cached cursors and connections.

        Args:
            exclude_calling_thread: If set to True excludes cursors and connections associated
                with the calling thread.
        """


class ThreadLocalConnectionPool(ConnectionPool):
    def __init__(self, connection_factory: t.Callable[[], t.Any]):
        self._connection_factory = connection_factory
        self._thread_connections: t.Dict[t.Hashable, t.Any] = {}
        self._thread_cursors: t.Dict[t.Hashable, t.Any] = {}
        self._thread_connections_lock = Lock()
        self._thread_cursors_lock = Lock()

    def get_cursor(self) -> t.Any:
        thread_id = get_ident()
        with self._thread_cursors_lock:
            if thread_id not in self._thread_cursors:
                self._thread_cursors[thread_id] = self.get().cursor()
            return self._thread_cursors[thread_id]

    def get(self) -> t.Any:
        thread_id = get_ident()
        with self._thread_connections_lock:
            if thread_id not in self._thread_connections:
                self._thread_connections[thread_id] = self._connection_factory()
            return self._thread_connections[thread_id]

    def close_cursor(self) -> None:
        thread_id = get_ident()
        with self._thread_cursors_lock:
            if thread_id in self._thread_cursors:
                _try_close(self._thread_cursors[thread_id], "cursor")
                self._thread_cursors.pop(thread_id)

    def close(self) -> None:
        thread_id = get_ident()
        with self._thread_cursors_lock, self._thread_connections_lock:
            if thread_id in self._thread_connections:
                _try_close(self._thread_connections[thread_id], "connection")
                self._thread_connections.pop(thread_id)
                self._thread_cursors.pop(thread_id, None)

    def close_all(self, exclude_calling_thread: bool = False) -> None:
        calling_thread_id = get_ident()
        with self._thread_cursors_lock, self._thread_connections_lock:
            for thread_id, connection in self._thread_connections.copy().items():
                if not exclude_calling_thread or thread_id != calling_thread_id:
                    # NOTE: the access to the connection instance itself is not thread-safe here.
                    _try_close(connection, "connection")
                    self._thread_connections.pop(thread_id)
                    self._thread_cursors.pop(thread_id, None)


class SingletonConnectionPool(ConnectionPool):
    def __init__(self, connection_factory: t.Callable[[], t.Any]):
        self._connection_factory = connection_factory
        self._connection: t.Optional[t.Any] = None
        self._cursor: t.Optional[t.Any] = None

    def get_cursor(self) -> t.Any:
        if not self._cursor:
            self._cursor = self.get().cursor()
        return self._cursor

    def get(self) -> t.Any:
        if not self._connection:
            self._connection = self._connection_factory()
        return self._connection

    def close_cursor(self) -> None:
        _try_close(self._cursor, "cursor")
        self._cursor = None

    def close(self) -> None:
        _try_close(self._connection, "connection")
        self._connection = None
        self._cursor = None

    def close_all(self, exclude_calling_thread: bool = False) -> None:
        if not exclude_calling_thread:
            self.close()


def connection_pool(
    connection_factory: t.Callable[[], t.Any], multithreaded: bool
) -> ConnectionPool:
    return (
        ThreadLocalConnectionPool(connection_factory)
        if multithreaded
        else SingletonConnectionPool(connection_factory)
    )


def _try_close(closeable: t.Any, kind: str) -> None:
    try:
        closeable.close()
    except Exception:
        logger.exception("Failed to close %s", kind)
