import asyncio
import json
import typing as t
from pathlib import Path

from fastapi.encoders import jsonable_encoder
from sse_starlette.sse import ServerSentEvent
from watchfiles import Change, DefaultFilter, awatch

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server import models
from web.server.api.endpoints.files import _get_directory, _get_file_with_content
from web.server.api.endpoints.models import get_all_models
from web.server.exceptions import ApiException
from web.server.settings import get_context, get_path_mapping, get_settings
from web.server.utils import is_relative_to


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = await get_context(settings)
    path_mapping = await get_path_mapping(settings)

    paths = [
        (context.path / c.MODELS).resolve(),
        (context.path / c.SEEDS).resolve(),
    ]
    ignore_entity_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    ignore_entity_patterns.append("^\\.DS_Store$")
    ignore_entity_patterns.append("^.*\.db(\.wal)?$")
    ignore_paths = [str((context.path / c.CACHE).resolve())]
    watch_filter = DefaultFilter(
        ignore_paths=ignore_paths, ignore_entity_patterns=ignore_entity_patterns
    )

    async for entries in awatch(context.path, watch_filter=watch_filter, force_polling=True):
        should_load_context = False
        changes: t.List[models.ArtifactChange] = []
        directories: t.Dict[str, models.Directory] = {}

        for change, path_str in entries:
            path = Path(path_str)
            relative_path = path.relative_to(settings.project_path)

            should_load_context = should_load_context or any(is_relative_to(path, p) for p in paths)

            if change == Change.modified and path.is_dir():
                directory = _get_directory(path, settings, path_mapping, context)
                directories[directory.path] = directory
            elif change == Change.deleted or not path.exists():
                changes.append(
                    models.ArtifactChange(
                        change=Change.deleted,
                        path=str(relative_path),
                    )
                )
            elif change == Change.modified and path.is_file():
                changes.append(
                    models.ArtifactChange(
                        type=models.ArtifactType.file,
                        change=change,
                        path=str(relative_path),
                        file=_get_file_with_content(relative_path, settings, path_mapping),
                    )
                )

        if should_load_context:
            reload_context(queue, context)

        queue.put_nowait(
            ServerSentEvent(
                event="file",
                data=json.dumps(
                    jsonable_encoder(
                        {
                            "changes": changes,
                            "directories": directories,
                        }
                    )
                ),
            )
        )


def reload_context(queue: asyncio.Queue, context: Context) -> None:
    try:
        context.load()
        queue.put_nowait(
            ServerSentEvent(
                event="models", data=json.dumps(jsonable_encoder(get_all_models(context)))
            )
        )
    except Exception:
        error = ApiException(
            message="Error refreshing the models while watching file changes",
            origin="API -> watcher -> reload_context",
        ).to_dict()
        queue.put_nowait(ServerSentEvent(event="errors", data=json.dumps(error)))
