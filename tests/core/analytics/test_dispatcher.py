import gzip
import json
import typing as t

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.analytics.dispatcher import AsyncEventDispatcher, EventEmitter
from sqlmesh.utils.errors import ApiClientError, SQLMeshError


@pytest.fixture
def dispatcher(mocker: MockerFixture) -> t.Iterator[AsyncEventDispatcher]:
    emitter_mock = mocker.Mock()
    dispatcher = AsyncEventDispatcher(
        emit_interval_sec=5, max_emit_interval_sec=15, emitter=emitter_mock
    )
    yield dispatcher
    dispatcher.shutdown(flush=False)


def test_emitter(mocker: MockerFixture):
    emitter = EventEmitter(base_url="http://localhost/v1/")
    emitter._session = mocker.Mock()
    emitter._session.post.return_value.status_code = 200  # type: ignore

    versions = json.dumps(emitter._versions)

    emitter.emit([{"name": "event_a"}, {"name": "event_b"}])
    emitter._session.post.assert_called_once_with(  # type: ignore
        "http://localhost/v1/sqlmesh/",
        data=gzip.compress(
            f'{{"events": [{{"name": "event_a"}}, {{"name": "event_b"}}], "versions": {versions}}}'.encode(
                "utf-8"
            )
        ),
        timeout=(10, 10),
    )


def test_dispatcher_flush(dispatcher: AsyncEventDispatcher):
    dispatcher.add_event({"name": "event_a"})
    dispatcher.add_event({"name": "event_b"})

    dispatcher.flush()

    dispatcher.emitter.emit.assert_called_once_with([{"name": "event_a"}, {"name": "event_b"}])  # type: ignore
    assert not dispatcher._events


def test_dispatcher_shutdown_with_flush(dispatcher: AsyncEventDispatcher):
    dispatcher.add_event({"name": "event_a"})
    dispatcher.add_event({"name": "event_b"})

    dispatcher.shutdown(flush=True)

    # Make sure that shutdown happens only once.
    dispatcher.shutdown(flush=True)

    dispatcher.emitter.emit.assert_called_once_with([{"name": "event_a"}, {"name": "event_b"}])  # type: ignore
    dispatcher.emitter.close.assert_called_once()  # type: ignore


def test_dispatcher_shutdown_no_flush(dispatcher: AsyncEventDispatcher):
    dispatcher.add_event({"name": "event_a"})
    dispatcher.add_event({"name": "event_b"})

    dispatcher.shutdown(flush=False)

    # Make sure that shutdown happens only once.
    dispatcher.shutdown(flush=False)

    dispatcher.emitter.emit.assert_not_called()  # type: ignore
    dispatcher.emitter.close.assert_called_once()  # type: ignore


def test_dispatcher_max_queue_size(dispatcher: AsyncEventDispatcher):
    dispatcher._max_queue_size = 3

    dispatcher.add_event({"name": "event_a"})
    dispatcher.add_event({"name": "event_b"})
    dispatcher.add_event({"name": "event_c"})
    dispatcher.add_event({"name": "event_d"})

    assert len(dispatcher._events) == 3

    dispatcher.flush()
    assert not dispatcher._events

    dispatcher.emitter.emit.side_effect = SQLMeshError("test_error")  # type: ignore

    dispatcher.add_event({"name": "event_a"})
    dispatcher.add_event({"name": "event_b"})
    dispatcher.add_event({"name": "event_c"})
    dispatcher.add_event({"name": "event_d"})

    dispatcher.flush()
    assert len(dispatcher._events) == 3


def test_dispatcher_too_many_requests_response(dispatcher: AsyncEventDispatcher):
    dispatcher.emitter.emit.side_effect = ApiClientError("test_error", 429)  # type: ignore

    dispatcher.add_event({"name": "event_a"})

    assert dispatcher._emit_interval_sec == 5

    dispatcher.flush()

    assert dispatcher._emit_interval_sec == 10

    dispatcher.flush()

    assert dispatcher._emit_interval_sec == 15

    dispatcher.flush()

    assert dispatcher._emit_interval_sec == 15


def test_dispatcher_upgrade_required_response(dispatcher: AsyncEventDispatcher):
    dispatcher.emitter.emit.side_effect = ApiClientError("test_error", 426)  # type: ignore

    dispatcher.add_event({"name": "event_a"})

    assert not dispatcher._shutdown_event.is_set()

    dispatcher.flush()

    assert dispatcher._shutdown_event.is_set()

    dispatcher.emitter.close.assert_called_once()  # type: ignore
