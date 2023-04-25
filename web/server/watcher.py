import asyncio
import json
from pathlib import Path

from watchfiles import Change, DefaultFilter, awatch

from sqlmesh.core import constants as c
from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)

    class IgnorePatternsFilter(DefaultFilter):
        ignore_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS

        def __call__(self, change: Change, path: str) -> bool:
            return super().__call__(change, path) and not any(
                Path(path).match(pattern) for pattern in self.ignore_patterns
            )

    async for _ in awatch((context.path / c.MODELS).resolve(), watch_filter=IgnorePatternsFilter()):
        context.load()

        queue.put_nowait(
            Event(event="models", data=json.dumps([model.dict() for model in get_models(context)]))
        )
