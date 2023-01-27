from __future__ import annotations

import os
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status

from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.settings import Settings, get_context, get_loaded_context, get_settings

router = APIRouter()


def validate_path(
    path: str,
    context: Context = Depends(get_context),
) -> str:
    resolved_path = context.path.resolve()
    full_path = (resolved_path / path).resolve()
    try:
        full_path.relative_to(resolved_path)
    except ValueError:
        raise HTTPException(status_code=404)

    if any(full_path.match(pattern) for pattern in context.ignore_patterns):
        raise HTTPException(status_code=404)

    return path


@router.get("/files")
def get_files(
    context: Context = Depends(get_context),
) -> models.Directory:
    """Get all project files."""

    def walk_path(
        path: str | Path,
    ) -> t.Tuple[t.List[models.Directory], t.List[models.File]]:
        directories = []
        files = []
        with os.scandir(path) as entries:
            for entry in entries:
                entry_path = Path(entry.path)
                if (
                    entry.name == "__pycache__"
                    or entry.name.startswith(".")
                    or any(
                        entry_path.match(pattern) for pattern in context.ignore_patterns
                    )
                ):
                    continue

                relative_path = os.path.relpath(entry.path, context.path)
                if entry.is_dir(follow_symlinks=False):
                    _directories, _files = walk_path(entry.path)
                    directories.append(
                        models.Directory(
                            name=entry.name,
                            path=relative_path,
                            directories=_directories,
                            files=_files,
                        )
                    )
                else:
                    files.append(models.File(
                        name=entry.name, path=relative_path))
        return sorted(directories, key=lambda x: x.name), sorted(
            files, key=lambda x: x.name
        )

    directories, files = walk_path(context.path)
    return models.Directory(
        name=os.path.basename(context.path),
        path="",
        directories=directories,
        files=files,
    )


@router.get("/files/{path:path}")
def get_file(
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> models.File:
    """Get a file, including its contents."""
    try:
        with open(settings.project_path / path) as f:
            content = f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    return models.File(name=os.path.basename(path), path=path, content=content)


@router.post("/files/{path:path}")
async def write_file(
    content: str = Body(),
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> models.File:
    """Create or update a file."""
    with open(settings.project_path / path, "w", encoding="utf-8") as f:
        f.write(content)
    return models.File(name=os.path.basename(path), path=path, content=content)


@router.delete("/files/{path:path}")
async def delete_file(
    response: Response,
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete a file."""
    try:
        (settings.project_path / path).unlink()
        response.status_code = status.HTTP_204_NO_CONTENT
    except FileNotFoundError:
        raise HTTPException(status_code=404)


@router.get(
    "/context",
    response_model_exclude_unset=True,
)
def get_api_context(
    context: Context = Depends(get_loaded_context),
) -> models.Context:
    """Get the context"""

    return models.Context(
        concurrent_tasks=context.concurrent_tasks,
        engine_adapter=context.engine_adapter.dialect,
        dialect=context.dialect,
        path=str(context.path),
        scheduler=context.config.scheduler.type_,
        time_column_format=context.config.time_column_format,
        models=list(context.models.keys()),
    )


@router.get(
    "/plan",
    response_model_exclude_unset=True,
)
def get_plan(
    environment: str = "",
    context: Context = Depends(get_loaded_context),
) -> models.ContextEnvironment:
    """Get the context for a environment."""

    plan = context.plan(environment=environment, no_prompts=True)
    batches = context.scheduler().batches()
    tasks = {snapshot.name: len(intervals) for snapshot, intervals in batches}

    payload = models.ContextEnvironment(
        environment=plan.environment.name,
        backfills=[
            (
                interval.snapshot_name,
                *[
                    [to_ds(t) for t in make_inclusive(start, end)]
                    for start, end in interval.merged_intervals
                ],
                tasks[interval.snapshot_name],
            )
            for interval in plan.missing_intervals
        ],
    )

    if plan.context_diff.has_differences:
        payload.changes = models.ContextEnvironmentChanges(
            removed=plan.context_diff.removed,
            added=plan.context_diff.added,
            modified=plan.context_diff.modified_snapshots,
        )

    return payload
