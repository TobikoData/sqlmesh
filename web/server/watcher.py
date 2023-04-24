import asyncio
import json
from pathlib import Path

from watchfiles import awatch

from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)

    def _get_path(path: str) -> str:
        return str(Path(path).relative_to(context.path))

    async for changes in awatch(settings.project_path):
        context.load()

        changes_paths = {_get_path(path) for type, path in changes if type == 2}
        changed_models = []
        output_models = []

        for model in get_models(context):
            if model.path in changes_paths:
                changed_models.append(model)
            output_models.append(model.dict())

        if changed_models:
            queue.put_nowait(Event(event="models", data=json.dumps(output_models)))
