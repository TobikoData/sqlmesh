from __future__ import annotations

import abc
import gzip
import json
import logging
import platform
import typing as t
from functools import cached_property
from threading import Event, Lock, Thread
from urllib.parse import urljoin

import requests
from sqlglot import __version__ as sqlglot_version

from sqlmesh.utils.errors import ApiClientError, raise_for_status

TOBIKO_COLLECTOR_URL = "https://analytics.tobikodata.com/v1/"

logger = logging.getLogger(__name__)


class EventEmitter:
    """Emits events to a remote collector."""

    def __init__(
        self,
        base_url: str = TOBIKO_COLLECTOR_URL,
        read_timeout_sec: int = 10,
        connect_timeout_sec: int = 10,
    ):
        from sqlmesh import __version__ as sqlmesh_version

        self.sqlmesh_url = urljoin(base_url, "sqlmesh/")
        self.read_timeout = read_timeout_sec
        self.connect_timeout = connect_timeout_sec

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
                "User-Agent": f"SQLMesh/{sqlmesh_version}",
            }
        )

    def emit(self, events: t.List[t.Dict[str, t.Any]]) -> None:
        data = json.dumps({"events": events, "versions": self._versions}).encode("utf-8")
        data = gzip.compress(data)
        response = self._session.post(
            self.sqlmesh_url, data=data, timeout=(self.connect_timeout, self.read_timeout)
        )
        raise_for_status(response)

    def close(self) -> None:
        self._session.close()

    @cached_property
    def _versions(self) -> str:
        import pydantic

        from sqlmesh import __version__ as sqlmesh_version

        versions = {
            "sqlmesh_version": sqlmesh_version,
            "sqlglot_version": sqlglot_version,
            "python_version": platform.python_version(),
            "pydantic_version": pydantic.__version__,
            "os_name": platform.system(),
            "platform": platform.platform(),
        }
        return json.dumps(versions)


class EventDispatcher(abc.ABC):
    """Dispatches events to an emitter."""

    @abc.abstractmethod
    def add_event(self, event: t.Dict[str, t.Any]) -> None:
        """Add an event to be emitted.

        Args:
            event: The event data.
        """

    @abc.abstractmethod
    def flush(self) -> None:
        """Flushes the collected events to the emitter."""

    @abc.abstractmethod
    def shutdown(self, flush: bool = True) -> None:
        """Shuts down this dispatcher and releases any resources.

        Args:
            flush: Whether to flush the collected events before shutting down.
        """


class AsyncEventDispatcher(EventDispatcher):
    def __init__(
        self,
        emit_interval_sec: int = 5,
        max_emit_interval_sec: int = 120,
        max_queue_size: int = 50,
        emitter: t.Optional[EventEmitter] = None,
    ):
        self._emit_interval_sec = emit_interval_sec
        self._max_emit_interval_sec = max_emit_interval_sec
        self._max_queue_size = max_queue_size
        self._emitter = emitter

        self._events: t.List[t.Dict[str, t.Any]] = []
        self._events_lock = Lock()

        self._shutdown_event = Event()
        self._emitter_thread = Thread(target=self._run_flush, name="event-emitter", daemon=True)
        self._emitter_thread.start()

    @cached_property
    def emitter(self) -> EventEmitter:
        return self._emitter or EventEmitter()

    def add_event(self, event: t.Dict[str, t.Any]) -> None:
        self._add_events([event])

    def flush(self) -> None:
        with self._events_lock:
            events_to_emit = self._events
            self._events = []

        if not events_to_emit:
            return

        logger.debug("Emitting %d events", len(events_to_emit))
        try:
            self.emitter.emit(events_to_emit)
        except Exception as e:
            logger.info("Failed to emit events: %s", e)
            if isinstance(e, ApiClientError):
                if e.code == 429 and self._emit_interval_sec < self._max_emit_interval_sec:
                    self._emit_interval_sec = min(
                        self._emit_interval_sec * 2, self._max_emit_interval_sec
                    )
                    logger.debug(
                        "Increasing the emit interval to %s seconds", self._emit_interval_sec
                    )
                elif e.code in (400, 403, 404, 405, 426):
                    logger.info(
                        "Non-retriable client error (%s) occurred, shutting down the event dispatcher",
                        e.code,
                    )
                    self.shutdown(flush=False)
                    return
            self._add_events(events_to_emit, prepend=True)

    def shutdown(self, flush: bool = True) -> None:
        if self._shutdown_event.is_set():
            return
        logging.info("Shutting down the event dispatcher")
        self._shutdown_event.set()
        self._emitter_thread.join()
        if flush and self._events:
            # Reduce the connect timeout to avoid blocking the shutdown.
            self.emitter.connect_timeout = 2
            self.flush()
        self.emitter.close()

    def _add_events(self, events: t.List[t.Dict[str, t.Any]], prepend: bool = False) -> None:
        with self._events_lock:
            if not prepend:
                self._events.extend(events)
            else:
                self._events = events + self._events

            if len(self._events) > self._max_queue_size:
                drop_count = len(self._events) - self._max_queue_size
                logger.info("Event queue is full, dropping %s events", drop_count)
                self._events = self._events[drop_count:]

    def _run_flush(self) -> None:
        while not self._shutdown_event.wait(self._emit_interval_sec):
            self.flush()


class NoopEventDispatcher(EventDispatcher):
    def add_event(self, event: t.Dict[str, t.Any]) -> None:
        logger.debug("Analytics is disabled, dropping event")

    def flush(self) -> None:
        pass

    def shutdown(self, flush: bool = True) -> None:
        pass
