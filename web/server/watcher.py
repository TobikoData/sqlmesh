import asyncio
import functools as ft
import json
import os
import typing as t
from pathlib import Path

from fastapi.encoders import jsonable_encoder
from watchfiles import Change, DefaultFilter, awatch

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server import models
from web.server.api.endpoints.files import _get_directory, _get_file_with_content
from web.server.api.endpoints.models import get_all_models
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context, get_path_mapping, get_settings
from web.server.sse import Event
from web.server.utils import is_relative_to


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_loaded_context(settings)
    path_mapping = await get_path_mapping(settings)

    paths = [
        (context.path / c.MODELS).resolve(),
        (context.path / c.SEEDS).resolve(),
    ]
    ignore_entity_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    ignore_entity_patterns.append("^\\.DS_Store$")
    ignore_paths = [str((context.path / c.CACHE).resolve())]
    watch_filter = DefaultFilter(
        ignore_paths=ignore_paths, ignore_entity_patterns=ignore_entity_patterns
    )

    async for entries in awatch(context.path, watch_filter=watch_filter, force_polling=True):
        should_load_context = False
        changes: t.List[models.ArtifactChange] = []

        for (change, path) in list(entries):
            if change == Change.modified and os.path.isdir(path):
                continue

            should_load_context = should_load_context or ft.reduce(
                lambda v, p: v or is_relative_to(Path(path), p), paths, False
            )

            relative_path = Path(path).relative_to(settings.project_path)

            if change == Change.deleted or os.path.exists(path) == False:
                changes.append(
                    models.ArtifactChange(
                        change=Change.deleted.name,
                        path=str(relative_path),
                    )
                )
            else:
                changes.append(
                    models.ArtifactChange(
                        type=models.ArtifactType.directory
                        if os.path.isdir(path)
                        else models.ArtifactType.file,
                        change=change.name,
                        path=str(relative_path),
                        directory=_get_directory(path, settings, path_mapping, context)
                        if os.path.isdir(path)
                        else None,
                        file=_get_file_with_content(relative_path, settings, path_mapping)
                        if os.path.isfile(path)
                        else None,
                    )
                )

        if should_load_context:
            _refresh_models(queue, context)

        queue.put_nowait(
            Event(
                event="file",
                data=json.dumps(jsonable_encoder(changes)),
            )
        )


def _refresh_models(queue: asyncio.Queue, context: Context) -> None:
    try:
        context.load()
        queue.put_nowait(
            Event(event="models", data=json.dumps(jsonable_encoder(get_all_models(context))))
        )
    except Exception:
        error = ApiException(
            message="Error refreshing the models while watching file changes",
            origin="API -> watcher -> _refresh_models",
        ).to_dict()
        queue.put_nowait(Event(event="errors", data=json.dumps(error)))
