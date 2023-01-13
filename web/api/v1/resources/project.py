from typing import List

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class File(BaseModel):
    id: int
    name: str


class Folder(BaseModel):
    id: int
    name: str
    files: List[File] | None = None
    folders: List["Folder"] | None = None


@router.get("/")
def projects():
    return {"ok": True, "status": 200}


@router.get("/{id}/structure")
def project_folders():
    return {
        "ok": True,
        "status": 200,
        "payload": Folder(
            id=1,
            name="wursthall",
            files=[File(id=1, name="config.yaml")],
            folders=[Folder(id=1, name="audits")],
        ),
    }
