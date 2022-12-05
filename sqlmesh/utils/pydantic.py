import typing as t

from pydantic import BaseModel
from sqlglot import exp

DEFAULT_ARGS = {"exclude_none": True, "by_alias": True}


class PydanticModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"
        json_encoders = {exp.Expression: lambda e: e.sql()}
        underscore_attrs_are_private = True
        smart_union = True

    def dict(
        self,
        **kwargs,
    ) -> t.Dict[str, t.Any]:
        return super().dict(**{**DEFAULT_ARGS, **kwargs})  # type: ignore

    def json(
        self,
        **kwargs,
    ) -> str:
        return super().json(**{**DEFAULT_ARGS, **kwargs})  # type: ignore
