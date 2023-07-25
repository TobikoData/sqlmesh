import asyncio
import typing as t

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse, ServerSentEvent

from sqlmesh.utils.date import now_timestamp

router = APIRouter()


@router.get("")
async def events(request: Request) -> EventSourceResponse:
    """SQLMesh console server sent events"""

    async def generator() -> t.AsyncGenerator:
        queue: asyncio.Queue = asyncio.Queue()
        request.app.state.console_listeners.append(queue)
        try:
            while True:
                yield await queue.get()
                queue.task_done()
        finally:
            request.app.state.console_listeners.remove(queue)

    return EventSourceResponse(
        generator(),
        ping=15,
        ping_message_factory=lambda: ServerSentEvent(
            event="ping", data={"timestamp": now_timestamp()}
        ),
    )
