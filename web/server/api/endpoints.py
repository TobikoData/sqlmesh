import os
import typing as t

from fastapi import APIRouter, Depends

from web.server.models import Directory, File
from web.server.settings import Settings, get_settings

router = APIRouter()


@router.get("/files")
def get_files(
    settings: Settings = Depends(get_settings),
) -> t.Tuple[t.List[Directory], t.List[File]]:
    """Get all project files."""

    def walk_path(path: str) -> t.Tuple[t.List[Directory], t.List[File]]:
        directories = []
        files = []
        for entry in os.scandir(path):
            if entry.name == "__pycache__" or entry.name.startswith("."):
                continue

            if entry.is_dir(follow_symlinks=False):
                _directories, _files = walk_path(entry.path)
                directories.append(
                    Directory(
                        name=entry.name,
                        path=entry.path,
                        directories=_directories,
                        files=_files,
                    )
                )
            else:
                files.append(File(name=entry.name, path=entry.path))
        return sorted(directories, key=lambda x: x.name), sorted(
            files, key=lambda x: x.name
        )

    return walk_path(settings.project_path)
