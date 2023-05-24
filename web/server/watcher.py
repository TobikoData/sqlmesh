import asyncio
import json
import logging

from fastapi import Depends
from fastapi.encoders import jsonable_encoder
from watchfiles import DefaultFilter, awatch

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server.api.endpoints.models import get_models
from web.server.exceptions import ApiException
from web.server.settings import get_context
from web.server.sse import Event

logger = logging.getLogger(__name__)


async def watch_project(queue: asyncio.Queue, context: Context = Depends(get_context)) -> None:
    path = (context.path / c.MODELS).resolve()
    ignore_entity_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    watch_filter = DefaultFilter(ignore_entity_patterns=ignore_entity_patterns)

    try:
        async for _ in awatch(path, watch_filter=watch_filter):
            queue.put_nowait(Event(event="models", data=json.dumps(jsonable_encoder(get_models()))))
    except Exception:
        exception = ApiException(
            message="Error watching file changes",
            origin="API -> watcher -> watch_project",
        )

        logger.error(exception)
        raise exception
