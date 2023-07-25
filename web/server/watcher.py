import asyncio
import json

from fastapi.encoders import jsonable_encoder
from sse_starlette.sse import ServerSentEvent
from watchfiles import DefaultFilter, awatch

from sqlmesh.core import constants as c
from web.server.api.endpoints.models import get_all_models
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context, get_settings


async def watch_project(queue: asyncio.Queue) -> None:
    context = await get_loaded_context(get_settings())
    paths = [(context.path / c.MODELS).resolve(), (context.path / c.SEEDS).resolve()]
    ignore_entity_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    watch_filter = DefaultFilter(ignore_entity_patterns=ignore_entity_patterns)

    async for _ in awatch(*paths, watch_filter=watch_filter):
        try:
            context.load()
        except Exception:
            queue.put_nowait(
                ServerSentEvent(
                    event="errors",
                    data=json.dumps(
                        ApiException(
                            message="Error watching file changes",
                            origin="API -> watcher -> watch_project",
                        ).to_dict()
                    ),
                )
            )
        else:
            queue.put_nowait(
                ServerSentEvent(
                    event="models", data=json.dumps(jsonable_encoder(get_all_models(context)))
                )
            )
