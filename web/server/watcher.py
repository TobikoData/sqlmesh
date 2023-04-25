import asyncio
import json

from watchfiles import DefaultFilter, awatch

from sqlmesh.core import constants as c
from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)

    async for _ in awatch(
        (context.path / c.MODELS).resolve(),
        watch_filter=DefaultFilter(
            ignore_entity_patterns=context.config.ignore_patterns if context else c.IGNORE_PATTERNS
        ),
    ):
        context.load()

        queue.put_nowait(
            Event(event="models", data=json.dumps([model.dict() for model in get_models(context)]))
        )
