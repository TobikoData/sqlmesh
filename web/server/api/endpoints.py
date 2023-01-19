import os
import typing as t

from fastapi import APIRouter, Depends

from web.server.models import Directory, File
from web.server.settings import Settings, get_settings

router = APIRouter()


@router.get("/files")
def get_files(
    settings: Settings = Depends(get_settings),
) -> Directory:
    """Get all project files."""

    def walk_path(path: str) -> t.Tuple[t.List[Directory], t.List[File]]:
        directories = []
        files = []
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.name == "__pycache__" or entry.name.startswith("."):
                    continue

                relative_path = os.path.relpath(entry.path, settings.project_path)
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

    directories, files = walk_path(settings.project_path)
    return Directory(
        name=os.path.basename(settings.project_path),
        path="",
        directories=directories,
        files=files,
    )
