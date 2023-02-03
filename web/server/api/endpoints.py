from __future__ import annotations

import asyncio
import functools
import json
import os
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from starlette.status import (
    HTTP_303_SEE_OTHER,
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_ENTITY,
)

from sqlmesh.core.console import ApiConsole
from sqlmesh.core.context import Context
from sqlmesh.utils.date import make_inclusive, to_ds
from web.server import models
from web.server.settings import Settings, get_context, get_loaded_context, get_settings
from web.server.sse import SSEResponse
from web.server.utils import run_in_executor, validate_path

SSE_DELAY = 1  # second
router = APIRouter()


@router.get("/files", response_model=models.Directory)
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


@router.get("/files/{path:path}", response_model=models.File)
def get_file(
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> models.File:
    """Get a file, including its contents."""
    try:
        with open(settings.project_path / path) as f:
            content = f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return models.File(name=os.path.basename(path), path=path, content=content)


@router.post("/files/{path:path}", response_model=models.File)
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
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    except IsADirectoryError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="File is a directory"
        )


@router.post("/directories/{path:path}", response_model=models.Directory)
async def create_directory(
    response: Response,
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> models.Directory:
    """Create a directory."""
    try:
        (settings.project_path / path).mkdir(parents=True)
        return models.Directory(name=os.path.basename(path), path=path)
    except FileExistsError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Directory already exists"
        )


@router.delete("/directories/{path:path}")
async def delete_directory(
    response: Response,
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete a directory."""
    try:
        (settings.project_path / path).rmdir()
        response.status_code = status.HTTP_204_NO_CONTENT
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    except NotADirectoryError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Not a directory"
        )
    except OSError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Directory not empty"
        )


@router.get(
    "/context",
    response_model=models.Context,
    response_model_exclude_unset=True,
)
def get_api_context(
    context: Context = Depends(get_loaded_context),
    settings: Settings = Depends(get_settings),
) -> models.Context:
    """Get the context"""

    context.refresh()

    return models.Context(
        concurrent_tasks=context.concurrent_tasks,
        engine_adapter=context.engine_adapter.dialect,
        scheduler=context.config.scheduler.type_,
        time_column_format=context.config.time_column_format,
        models=list(context.models.keys()),
        config=settings.config,
    )


@router.get(
    "/plan",
    response_model=models.ContextEnvironment,
    response_model_exclude_unset=True,
)
def get_plan(
    environment: str,
    context: Context = Depends(get_loaded_context),
) -> models.ContextEnvironment:
    """Get the context for a environment."""

    plan = context.plan(environment=environment, no_prompts=True)
    payload = models.ContextEnvironment(
        environment=plan.environment.name,
    )

    if plan.context_diff.has_differences:
        batches = context.scheduler().batches()
        tasks = {
            snapshot.name: len(intervals) for snapshot, intervals in batches.items()
        }

        payload.backfills = [
            models.ContextEnvironmentBackfill(
                model_name=interval.snapshot_name,
                interval=[
                    [to_ds(t) for t in make_inclusive(start, end)]
                    for start, end in interval.merged_intervals
                ][0],
                batches=tasks[interval.snapshot_name],
            )
            for interval in plan.missing_intervals
        ]

        payload.changes = models.ContextEnvironmentChanges(
            removed=plan.context_diff.removed,
            added=plan.context_diff.added,
            modified=models.ModelsDiff.get_modified_snapshots(
                plan.context_diff),
        )

    return payload


@router.post("/apply")
async def apply(
    environment: str,
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> RedirectResponse:
    """Apply a plan"""
    plan = functools.partial(
        context.plan, environment, no_prompts=True, auto_apply=True
    )

    if not hasattr(request.app.state, "task") or request.app.state.task.done():
        task = asyncio.create_task(run_in_executor(plan))
        setattr(task, "_environment", environment)
        request.app.state.task = task

    return {"ok": True}


@router.get("/tasks")
async def tasks(
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> SSEResponse:
    """Stream of plan application events"""
    task = None
    environment = None

    if hasattr(request.app.state, "task"):
        task = request.app.state.task
        environment = getattr(task, "_environment", None)

    def create_response(task_status: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        data = json.dumps(
            {
                "ok": True,
                "environment": environment,
                "tasks": task_status,
            }
        )

        return {"data": f"{data}"}

    async def running_tasks() -> t.AsyncGenerator:
        console: ApiConsole = context.console  # type: ignore
        if task:
            while not task.done():
                task_status = console.current_task_status
                if task_status:
                    yield create_response(task_status)
                await asyncio.sleep(SSE_DELAY)
            task_status = console.previous_task_status
            if task_status:
                yield create_response(task_status)
        yield create_response({})

    return SSEResponse(running_tasks())


@router.post("/plan/cancel")
async def cancel(
    request: Request,
    response: Response,
    context: Context = Depends(get_loaded_context),
) -> None:
    """Cancel a plan application"""
    if not hasattr(request.app.state, "task") or not request.app.state.task.cancel():
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="No active task found."
        )
    response.status_code = status.HTTP_204_NO_CONTENT
