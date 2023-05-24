import asyncio
import json
import logging

from fastapi.encoders import jsonable_encoder
from watchfiles import DefaultFilter, awatch

from sqlmesh.core import constants as c
from web.server.api.endpoints.models import get_models
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context, get_settings
from web.server.sse import Event

logger = logging.getLogger(__name__)


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)
    path = (context.path / c.MODELS).resolve()
    ignore_entity_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    watch_filter = DefaultFilter(ignore_entity_patterns=ignore_entity_patterns)

    async for _ in awatch(path, watch_filter=watch_filter):
        try:
            context.load()
        except Exception:
            queue.put_nowait(
                Event(
                    event="errors",
                    data=json.dumps(
                        ApiException(
                            message="File change triggered an error",
                            origin="API -> watcher -> watch_project",
                        ).to_dict()
                    ),
                )
            )
        else:
            queue.put_nowait(
                Event(event="models", data=json.dumps(jsonable_encoder(get_models(context))))
            )
