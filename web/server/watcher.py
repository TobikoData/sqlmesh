import typing as t
from pathlib import Path

from watchfiles import Change, DefaultFilter, awatch

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server import models
from web.server.api.endpoints.files import _get_directory, _get_file_with_content
from web.server.api.endpoints.models import serialize_all_models
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import (
    _get_path_to_model_mapping,
    get_context_or_raise,
    get_settings,
)
from web.server.utils import is_relative_to, run_in_executor


async def watch_project() -> None:
    settings = get_settings()
    context = await get_context_or_raise(settings)

    paths = [
        (context.path / c.MODELS).resolve(),
        (context.path / c.SEEDS).resolve(),
        (context.path / c.TESTS).resolve(),
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
            try:
                relative_path = path.relative_to(settings.project_path)
                should_load_context = should_load_context or any(
                    is_relative_to(path, p) for p in paths
                )

                if change == Change.modified and path.is_dir():
                    directory = _get_directory(path, settings, context)
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
                            file=_get_file_with_content(
                                settings.project_path / relative_path, str(relative_path)
                            ),
                        )
                    )
            except Exception:
                error = ApiException(
                    message="Error updating file",
                    origin=f"API -> watcher -> watch_project",
                    trigger=path_str,
                ).to_dict()
                api_console.log_event(event=models.EventName.WARNINGS, data=error)

        if settings.modules.intersection({models.Modules.FILES, models.Modules.DOCS}):
            api_console.log_event(
                event=models.EventName.FILE,
                data={
                    "changes": changes,
                    "directories": directories,
                },
            )

        if should_load_context:
            await run_in_executor(reload_context_and_update_models, context, path)


def reload_context_and_update_models(context: Context, change_path: Path) -> None:
    try:
        context.load()
        path_to_model_mapping = _get_path_to_model_mapping(context)
        maybe_model = path_to_model_mapping.get(change_path)
        api_console.log_event(
            event=models.EventName.MODELS,
            data=serialize_all_models(context, {maybe_model.name} if maybe_model else set()),
        )
    except Exception:
        error = ApiException(
            message="Error refreshing the models while watching file changes",
            origin="API -> watcher -> reload_context",
        ).to_dict()
        api_console.log_event(event=models.EventName.ERRORS, data=error)
