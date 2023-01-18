import os
import pathlib
import random
import string
import typing as t

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class File(BaseModel):
    id: str
    is_supported: bool = True
    name: str
    extension: str
    value: str


class Folder(BaseModel):
    id: str
    name: str
    folders: t.List["Folder"]
    files: t.List[File]


@router.get("/")
def projects():
    return {"ok": True, "status": 200}


@router.get("/{id}/structure")
def project_folders():
    (folders, files) = file_browser("../../example")

    return {
        "ok": True,
        "status": 200,
        "payload": {
            "folders": folders,
            "files": files,
        },
    }


SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml"}


def file_browser(path: str) -> t.Tuple[t.List[Folder], t.List[File]]:
    folders = []
    files = []

    for entry in os.scandir(path):
        if (
            entry.name == "__pycache__"
            or entry.name == "__init__.py"
            or entry.name.startswith(".")
        ):
            continue

        if entry.is_dir(follow_symlinks=False):
            (subfolders, subfiles) = file_browser(entry.path)

            folders.append(
                Folder(
                    id=id(),
                    name=entry.name,
                    folders=subfolders,
                    files=subfiles,
                )
            )
        else:

            extension = pathlib.Path(entry.path).suffix

            if extension in SUPPORTED_EXTENSIONS:
                if os.path.exists(entry.path):
                    with open(entry.path, "r") as f:
                        files.append(
                            File(
                                id=id(),
                                is_supported=extension in SUPPORTED_EXTENSIONS,
                                name=entry.name,
                                extension=extension,
                                value=f.read(),
                            )
                        )
                else:
                    print("File does not exist.")
            else:
                files.append(
                    File(
                        id=id(),
                        is_supported=False,
                        name=entry.name,
                        extension=extension,
                        value="",
                    )
                )

    return (folders, files)


def id(length=10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))
