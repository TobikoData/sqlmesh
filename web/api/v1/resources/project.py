import os
import pathlib
import typing as t

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class File(BaseModel):
    id: int
    is_supported: bool = True
    name: str


class Folder(BaseModel):
    id: int
    name: str
    folders: t.List["Folder"]
    files: t.List[File]


@router.get("/")
def projects():
    return {"ok": True, "status": 200}


@router.get("/{id}/structure")
def project_folders():
    (folders, files) = file_browser("example")

    return {
        "ok": True,
        "status": 200,
        "payload": {
            "folders": folders,
            "files": files,
        },
    }


suppurted_extensions = {".py", ".sql", ".yaml"}
suppurted_folders = {"audits", "tests", "models", "macros"}


def file_browser(path: str) -> t.Tuple[t.List[Folder], t.List[File]]:
    folders = []
    files = []

    id_dir = 0
    id_file = 0

    for entry in os.scandir(path):
        if (
            entry.name == "__pycache__"
            or entry.name == "__init__.py"
            or entry.name.startswith(".")
        ):
            continue

        if entry.is_dir(follow_symlinks=False):
            id_dir += 1

            (subfolders, subfiles) = file_browser(entry.path)

            folders.append(
                Folder(
                    id=id_dir,
                    name=entry.name,
                    folders=subfolders,
                    files=subfiles,
                )
            )
        else:
            id_file += 1

            extension = pathlib.Path(entry.path).suffix

            files.append(
                File(
                    id=id_file,
                    is_supported=extension in suppurted_extensions,
                    name=entry.name,
                )
            )

    return (folders, files)
