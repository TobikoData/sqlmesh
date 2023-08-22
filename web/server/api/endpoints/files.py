from __future__ import annotations

import json
import os
import pathlib
import typing as t
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Response
from sse_starlette import ServerSentEvent
from starlette.status import HTTP_204_NO_CONTENT, HTTP_404_NOT_FOUND

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import format_model_expressions, parse
from web.server import models
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import (
    Settings,
    get_context,
    get_path_mapping,
    get_path_to_model_mapping,
    get_settings,
)
from web.server.utils import is_relative_to, replace_file, validate_path

router = APIRouter()


@router.get("", response_model=models.Directory)
def get_files(
    context: t.Optional[Context] = Depends(get_context),
    settings: Settings = Depends(get_settings),
    path_mapping: t.Dict[Path, models.FileType] = Depends(get_path_mapping),
) -> models.Directory:
    """Get all project files."""
    return _get_directory(
        path=settings.project_path,
        settings=settings,
        path_mapping=path_mapping,
        context=context,
    )


@router.get("/{path:path}", response_model=models.File)
def get_file(
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
    path_mapping: t.Dict[Path, models.FileType] = Depends(get_path_mapping),
) -> models.File:
    """Get a file, including its contents."""
    try:
        file_path = Path(path)
        file = _get_file_with_content(file_path, settings, path_mapping)
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    return file


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
        full_path = settings.project_path / path
        if pathlib.Path(path_or_new_path).suffix == ".sql":
            path_to_model_mapping = await get_path_to_model_mapping(settings=settings)
            model = path_to_model_mapping.get(Path(full_path))
            default_dialect = context.config_for_path(Path(path_or_new_path)).dialect
            dialect = model.dialect if model and model.is_sql else default_dialect

            try:
                expressions = parse(content, default_dialect=default_dialect)
                content = format_model_expressions(expressions, dialect)
            except Exception:
                error = ApiException(
                    message="Unable to format SQL file",
                    origin="API -> files -> write_file",
                ).to_dict()
                api_console.queue.put_nowait(
                    ServerSentEvent(event="errors", data=json.dumps(error))
                )

        full_path.write_text(content, encoding="utf-8")

    path_or_new_path_mapping = await get_path_mapping(settings=settings)
    content = (settings.project_path / path_or_new_path).read_text()
    return models.File(
        name=os.path.basename(path_or_new_path),
        path=path_or_new_path,
        content=content,
        type=path_or_new_path_mapping.get(Path(path_or_new_path)),
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


def _get_directory(
    path: str | Path,
    settings: Settings,
    path_mapping: t.Dict[Path, models.FileType],
    context: t.Optional[Context] = None,
) -> models.Directory:
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
                elif entry.is_file(follow_symlinks=False):
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

    directories, files = walk_path(path)
    relative_path = str(Path(path).relative_to(settings.project_path))
    return models.Directory(
        name=os.path.basename(path),
        path="" if relative_path == "." else relative_path,
        directories=directories,
        files=files,
    )


def _get_file_with_content(
    path: Path,
    settings: Settings,
    path_mapping: t.Dict[Path, models.FileType],
) -> models.File:
    """Get a file, including its contents."""
    file_path = settings.project_path / path

    with open(file_path) as f:
        content = f.read()

    return models.File(
        name=path.name,
        path=str(path),
        content=content,
        type=path_mapping.get(path),
    )
