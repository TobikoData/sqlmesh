import asyncio
import typing as t

from fastapi import APIRouter, Request

from web.server.sse import SSEResponse

router = APIRouter()


@router.get("")
async def events(request: Request) -> SSEResponse:
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

    return SSEResponse(generator())
