import asyncio
import json
import typing as t
from pathlib import Path

from watchfiles import awatch

from sqlmesh.core.context import Context
from web.server.api.endpoints.models import get_models
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)

    _get_path = _to_relative_path(context)

    async for changes in awatch(settings.project_path):
        context.load()

        changed_models = []
        output_models = []
        changes_path = {_get_path(path) for type, path in changes if type == 2}

        for model in get_models(context):
            if model.path in changes_path:
                changed_models.append(model)
            output_models.append(model.dict())

        if changed_models:
            queue.put_nowait(Event(event="models", data=json.dumps(output_models)))


def _to_relative_path(context: Context) -> t.Callable[[str], str]:
    return lambda path: str(Path(path).relative_to(context.path))
