from __future__ import annotations

import pathlib
import typing as t

from pydantic import validator

from sqlmesh.utils.pydantic import PydanticModel

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml"}


class File(PydanticModel):
    name: str
    path: str
    extension: str = ""
    is_supported: bool = False
    content: t.Optional[str]

    @validator("extension", always=True)
    def default_extension(cls, v: str, values: t.Dict[str, t.Any]) -> str:
        if "name" in values:
            return pathlib.Path(values["name"]).suffix
        return v

    @validator("is_supported", always=True)
    def default_is_supported(cls, v: bool, values: t.Dict[str, t.Any]) -> bool:
        if "extension" in values:
            return values["extension"] in SUPPORTED_EXTENSIONS
        return v


class Directory(PydanticModel):
    name: str
    path: str
    directories: t.List[Directory] = []
    files: t.List[File] = []
