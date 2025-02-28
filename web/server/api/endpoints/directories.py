import os
import shutil
import typing as t

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status
from starlette.status import HTTP_404_NOT_FOUND

from web.server import models
from web.server.exceptions import ApiException
from web.server.settings import Settings, get_settings
from web.server.utils import replace_file, validate_path

router = APIRouter()


@router.post("/{path:path}", response_model=models.Directory)
async def write_directory(
    path: str = Depends(validate_path),
    new_path: t.Optional[str] = Body(None, embed=True),
    settings: Settings = Depends(get_settings),
) -> models.Directory:
    """Create or rename a directory."""
    if new_path:
        new_path = validate_path(new_path, settings)
        replace_file(settings.project_path / path, settings.project_path / new_path)
        return models.Directory(name=os.path.basename(new_path), path=new_path)

    try:
        (settings.project_path / path).mkdir(parents=True)
        return models.Directory(name=os.path.basename(path), path=path)
    except FileExistsError:
        raise ApiException(
            message="Directory already exists",
            origin="API -> directories -> write_directory",
        )


@router.delete("/{path:path}")
async def delete_directory(
    response: Response,
    path: str = Depends(validate_path),
    settings: Settings = Depends(get_settings),
) -> None:
    """Delete a directory."""
    try:
        shutil.rmtree(settings.project_path / path)
        response.status_code = status.HTTP_204_NO_CONTENT
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    except NotADirectoryError:
        raise ApiException(
            message="Not a directory",
            origin="API -> directories -> delete_directory",
        )
