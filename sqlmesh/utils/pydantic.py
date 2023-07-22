import typing as t

from pydantic import BaseModel
from sqlglot import exp

DEFAULT_ARGS = {"exclude_none": True, "by_alias": True}


class PydanticModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"
        json_encoders = {
            exp.Expression: lambda e: e.meta.get("sql") or e.sql(dialect=e.meta.get("dialect"))
        }
        underscore_attrs_are_private = True
        smart_union = True

    def dict(
        self,
        **kwargs: t.Any,
    ) -> t.Dict[str, t.Any]:
        return super().dict(**{**DEFAULT_ARGS, **kwargs})  # type: ignore

    def json(
        self,
        **kwargs: t.Any,
    ) -> str:
        return super().json(**{**DEFAULT_ARGS, **kwargs})  # type: ignore

    @classmethod
    def missing_required_fields(
        cls: t.Type["PydanticModel"], provided_fields: t.Set[str]
    ) -> t.Set[str]:
        return cls.required_fields() - provided_fields

    @classmethod
    def extra_fields(cls: t.Type["PydanticModel"], provided_fields: t.Set[str]) -> t.Set[str]:
        return provided_fields - cls.all_fields()

    @classmethod
    def all_fields(cls: t.Type["PydanticModel"]) -> t.Set[str]:
        return cls._fields()

    @classmethod
    def required_fields(cls: t.Type["PydanticModel"]) -> t.Set[str]:
        return cls._fields(lambda field: field.required)

    @classmethod
    def _fields(
        cls: t.Type["PydanticModel"],
        predicate: t.Callable[[t.Any], bool] = lambda _: True,
    ) -> t.Set[str]:
        return {
            field.alias if field.alias else field.name
            for field in cls.__fields__.values()
            if predicate(field)
        }
