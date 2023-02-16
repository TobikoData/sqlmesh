import os
import shutil

from fastapi import APIRouter, Depends, HTTPException, Response, status
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from web.server import models
from web.server.settings import Settings, get_settings
from web.server.utils import validate_path

router = APIRouter()


@router.post("/{path:path}", response_model=models.Directory)
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
