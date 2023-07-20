import asyncio
import functools as ft
import json
import os
import typing as t
from pathlib import Path

from fastapi.encoders import jsonable_encoder
from watchfiles import Change, DefaultFilter, awatch

from sqlmesh.core import constants as c
from web.server import models
from web.server.api.endpoints.files import _get_directory, _get_file
from web.server.api.endpoints.models import get_all_models
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context, get_path_mapping, get_settings
from web.server.sse import Event


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

    async for entries in awatch(
        context.path,
        watch_filter=watch_filter,
        force_polling=True,
    ):
        should_load_context = False
        changes: t.List[models.ArtifactChange] = []
        all_models: t.Optional[t.List[models.Model]] = None

        for (change, path) in list(entries):
            if change == Change.modified and os.path.isdir(path):
                continue

            if should_load_context == False and ft.reduce(
                lambda v, p: v or Path(path).is_relative_to(p), paths, False
            ):
                should_load_context = True

            relative_path = Path(path).relative_to(settings.project_path)

            if change == Change.deleted or os.path.exists(path) == False:
                changes.append(
                    models.ArtifactChange(
                        change=Change.deleted.name,
                        path=str(relative_path),
                    )
                )

                continue

            if os.path.isdir(path):
                changes.append(
                    models.ArtifactChange(
                        type=models.ArtifactType.directory,
                        change=change.name,
                        path=str(relative_path),
                        directory=_get_directory(Path(path), settings, path_mapping, context),
                    )
                )

            if os.path.isfile(path):
                changes.append(
                    models.ArtifactChange(
                        type=models.ArtifactType.file,
                        change=change.name,
                        path=str(relative_path),
                        file=_get_file(relative_path, settings, path_mapping),
                    )
                )

        if should_load_context:
            try:
                context.load()
            except Exception as e:
                error = ApiException(
                    message="Error watching file changes",
                    origin="API -> watcher -> watch_project",
                ).to_dict()
                queue.put_nowait(Event(event="errors", data=json.dumps(error)))
                raise e
            all_models = get_all_models(context)

        payload = models.ArtifactChanges(
            changes=changes,
            models=all_models,
        )

        queue.put_nowait(
            Event(
                event="file",
                data=json.dumps(
                    jsonable_encoder(payload.dict(exclude_none=True, exclude_unset=True))
                ),
            )
        )
