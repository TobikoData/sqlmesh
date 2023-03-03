from __future__ import annotations

import os
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server import models
from web.server.settings import Settings, get_context, get_settings
from web.server.utils import replace_file, validate_path

router = APIRouter()


@router.get("", response_model=models.Directory)
def get_files(
    context: t.Optional[Context] = Depends(get_context),
    settings: Settings = Depends(get_settings),
) -> models.Directory:
    """Get all project files."""
    ignore_patterns = context.ignore_patterns if context else c.IGNORE_PATTERNS

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
                    or any(entry_path.match(pattern) for pattern in ignore_patterns)
                ):
                    continue

                relative_path = os.path.relpath(entry.path, settings.project_path)
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
                    files.append(models.File(name=entry.name, path=relative_path))
        return sorted(directories, key=lambda x: x.name), sorted(files, key=lambda x: x.name)

    directories, files = walk_path(settings.project_path)
    return models.Directory(
        name=os.path.basename(settings.project_path),
        path="",
        directories=directories,
        files=files,
    )


@router.get("/{path:path}", response_model=models.File)
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


@router.post("/{path:path}", response_model=models.File)
async def write_file(
    content: str = Body("", embed=True),
    new_path: t.Optional[str] = Body(None, embed=True),
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
    context: Context = Depends(get_context),
) -> models.File:
    """Create, update, or rename a file."""
    path_or_new_path = path
    if new_path:
        path_or_new_path = validate_path(new_path, context)
        replace_file(settings.project_path / path, settings.project_path / path_or_new_path)
    else:
        (settings.project_path / path_or_new_path).write_text(content, encoding="utf-8")

    content = (settings.project_path / path_or_new_path).read_text()
    return models.File(
        name=os.path.basename(path_or_new_path), path=path_or_new_path, content=content
    )


@router.delete("/{path:path}")
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
        raise HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="File is a directory")
