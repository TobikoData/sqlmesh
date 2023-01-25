from __future__ import annotations

import os
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status

from sqlmesh.core.context import Context
from web.server.models import Directory, File
from web.server.settings import Settings, get_context, get_settings

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
) -> Directory:
    """Get all project files."""

    def walk_path(path: str | Path) -> t.Tuple[t.List[Directory], t.List[File]]:
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
                        Directory(
                            name=entry.name,
                            path=relative_path,
                            directories=_directories,
                            files=_files,
                        )
                    )
                else:
                    files.append(File(name=entry.name, path=relative_path))
        return sorted(directories, key=lambda x: x.name), sorted(
            files, key=lambda x: x.name
        )

    directories, files = walk_path(context.path)
    return Directory(
        name=os.path.basename(context.path),
        path="",
        directories=directories,
        files=files,
    )


@router.get("/files/{path:path}")
def get_file(
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> File:
    """Get a file, including its contents."""
    try:
        with open(settings.project_path / path) as f:
            content = f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404)
    return File(name=os.path.basename(path), path=path, content=content)


@router.post("/files/{path:path}")
async def write_file(
    content: str = Body(),
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> File:
    """Create or update a file."""
    with open(settings.project_path / path, "w", encoding="utf-8") as f:
        f.write(content)
    return File(name=os.path.basename(path), path=path, content=content)


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


@router.get("/context")
def get_context(
    settings: Settings = Depends(get_settings),
) -> t.Dict[str, t.Any]:
    """Get the context"""

    return {
        "concurrent_tasks": settings.context.concurrent_tasks,
        "models": list(map(lambda x: x.name, settings.context.models.values())),
        "engine_adapter": settings.context.engine_adapter.dialect,
        "dialect": settings.context.dialect,
        "path": settings.context.path,
        "scheduler": settings.context.config.scheduler.type_,
        "users": settings.context.config.users,
        "time_column_format": settings.context.config.time_column_format,
    }


@router.get("/context/{environment:path}")
def get_context_by_environment(
    environment: str = "",
    settings: Settings = Depends(get_settings),
) -> t.Dict[str, t.Any]:
    """Get the context for a environment."""

    plan = settings.context.plan(environment=environment, no_prompts=True)
    backfills = map(
        lambda x: (x.snapshot_name, x.format_missing_range()
                   ), plan.missing_intervals
    )
    payload = {
        "environment": plan.environment.name,
        "backfills": list(backfills),
    }  # type: t.Dict[str, t.Any]

    if plan.context_diff.has_differences:
        payload["changes"] = {
            "removed": plan.context_diff.removed,
            "added": plan.context_diff.added,
            "modified": plan.context_diff.modified_snapshots,
        }

    return payload
