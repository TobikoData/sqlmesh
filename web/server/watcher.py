import asyncio
import json

from watchfiles import awatch

from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)

    async for changes in awatch(settings.project_path):
        context.load()

        queue.put_nowait(
            Event(event="models", data=json.dumps([model.dict() for model in get_models(context)]))
        )
        queue.put_nowait(
            Event(
                event="lineage",
                data=json.dumps({name: list(models) for name, models in context.dag.graph.items()}),
            )
        )
