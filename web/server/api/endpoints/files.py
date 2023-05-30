from __future__ import annotations

import os
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Response
from starlette.status import HTTP_204_NO_CONTENT, HTTP_404_NOT_FOUND

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from web.server import models
from web.server.exceptions import ApiException
from web.server.settings import Settings, get_context, get_path_mapping, get_settings
from web.server.utils import is_relative_to, replace_file, validate_path

router = APIRouter()


@router.get("", response_model=models.Directory)
def get_files(
    context: t.Optional[Context] = Depends(get_context),
    settings: Settings = Depends(get_settings),
    path_mapping: t.Dict[Path, models.FileType] = Depends(get_path_mapping),
) -> models.Directory:
    """Get all project files."""
    ignore_patterns = context.config.ignore_patterns if context else c.IGNORE_PATTERNS
    macro_directory_path = Path(c.MACROS)
    test_directory_path = Path(c.TESTS)

    def walk_path(
        path: str | Path,
    ) -> tuple[list[models.Directory], list[models.File]]:
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

                relative_path = entry_path.relative_to(settings.project_path)
                if entry.is_dir(follow_symlinks=False):
                    _directories, _files = walk_path(entry.path)
                    directories.append(
                        models.Directory(
                            name=entry.name,
                            path=str(relative_path),
                            directories=_directories,
                            files=_files,
                        )
                    )
                else:
                    file_type = None
                    if is_relative_to(relative_path, macro_directory_path):
                        file_type = models.FileType.macros
                    elif is_relative_to(relative_path, test_directory_path):
                        file_type = models.FileType.tests
                    else:
                        file_type = path_mapping.get(relative_path)
                    files.append(
                        models.File(
                            name=entry.name,
                            path=str(relative_path),
                            type=file_type,
                        )
                    )
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
    path_mapping: t.Dict[Path, models.FileType] = Depends(get_path_mapping),
) -> models.File:
    """Get a file, including its contents."""
    try:
        with open(settings.project_path / path) as f:
            content = f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return models.File(
        name=os.path.basename(path), path=path, content=content, type=path_mapping.get(Path(path))
    )


@router.post("/{path:path}", response_model=models.File)
async def write_file(
    content: str = Body("", embed=True),
    new_path: t.Optional[str] = Body(None, embed=True),
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
    context: Context = Depends(get_context),
    path_mapping: t.Dict[Path, models.FileType] = Depends(get_path_mapping),
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
        name=os.path.basename(path_or_new_path),
        path=path_or_new_path,
        content=content,
        type=path_mapping.get(Path(path)),
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
        response.status_code = HTTP_204_NO_CONTENT
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    except IsADirectoryError:
        raise ApiException(
            message="File is a directory",
            origin="API -> files -> delete_file",
        )
