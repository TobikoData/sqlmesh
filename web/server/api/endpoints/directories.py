import os
import shutil
import traceback
import typing as t

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server import models
from web.server.settings import Settings, get_context, get_settings
from web.server.utils import validate_path

router = APIRouter()


@router.post("/{path:path}", response_model=models.Directory)
async def write_directory(
    response: Response,
    path: str = Depends(validate_path),
    new_path: t.Optional[str] = Body(None, embed=True),
    settings: Settings = Depends(get_settings),
    context: Context = Depends(get_context),
) -> models.Directory:
    """Create or rename a directory."""
    if new_path and new_path != path:
        validate_path(new_path, context)
        try:
            (settings.project_path / path).replace(settings.project_path / new_path)
        except FileNotFoundError:
            raise HTTPException(status_code=HTTP_404_NOT_FOUND)
        except OSError:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
            )
        return models.Directory(name=os.path.basename(new_path), path=new_path)

    try:
        (settings.project_path / path).mkdir(parents=True)
        return models.Directory(name=os.path.basename(path), path=path)
    except FileExistsError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Directory already exists"
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
        raise HTTPException(status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail="Not a directory")
