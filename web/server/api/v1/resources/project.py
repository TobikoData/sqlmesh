import hashlib
import os
import pathlib
import typing as t

from fastapi import APIRouter

from sqlmesh.utils.pydantic import PydanticModel

router = APIRouter()


class File(PydanticModel):
    id: str
    is_supported: bool = True
    name: str
    extension: str
    path: str
    value: str = ""


class Folder(PydanticModel):
    id: str
    name: str
    folders: t.List["Folder"]
    files: t.List[File]
    path: str


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
                    id=get_id(entry.path),
                    name=entry.name,
                    folders=subfolders,
                    files=subfiles,
                    path=entry.path,
                )
            )
        else:
            extension = pathlib.Path(entry.path).suffix

            if extension in SUPPORTED_EXTENSIONS:
                with open(entry.path, "r") as f:
                    files.append(
                        File(
                            id=get_id(entry.path),
                            is_supported=True,
                            name=entry.name,
                            extension=extension,
                            value=f.read(),
                            path=entry.path,
                        )
                    )
            else:
                files.append(
                    File(
                        id=get_id(entry.path),
                        is_supported=False,
                        name=entry.name,
                        extension=extension,
                        path=entry.path,
                    )
                )

    return (folders, files)


def get_id(path: str) -> str:
    result = hashlib.blake2b(path.encode())

    return result.hexdigest()
