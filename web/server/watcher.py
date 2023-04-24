import asyncio
import json
from pathlib import Path

from watchfiles import awatch

from sqlmesh.core import constants as c
from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)
    ignore_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS

    async for changes in awatch((context.path / "models").resolve()):
        context.load()

        output_models = []

        for _, path in changes:
            if any(Path(path).match(pattern) for pattern in ignore_patterns):
                continue

            for model in get_models(context):
                output_models.append(model.dict())

        queue.put_nowait(Event(event="models", data=json.dumps(output_models)))
