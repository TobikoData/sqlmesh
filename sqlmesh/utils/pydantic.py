import sys
import typing as t
from functools import wraps

import pydantic
from pydantic.fields import FieldInfo
from sqlglot import exp

from sqlmesh.core import dialect as d
from sqlmesh.utils import str_to_bool

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated

if t.TYPE_CHECKING:
    Model = t.TypeVar("Model", bound="PydanticModel")


DEFAULT_ARGS = {"exclude_none": True, "by_alias": True}


PYDANTIC_MAJOR_VERSION = int(pydantic.__version__.split(".")[0])


if PYDANTIC_MAJOR_VERSION >= 2:

    def field_validator(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        # Pydantic v2 doesn't support "always" argument. The validator behaves as if "always" is True.
        kwargs.pop("always", None)
        return pydantic.field_validator(*args, **kwargs)  # type: ignore

    def model_validator(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        # Pydantic v2 doesn't support "always" argument. The validator behaves as if "always" is True.
        kwargs.pop("always", None)
        return pydantic.model_validator(*args, **kwargs)  # type: ignore

    def field_serializer(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        return pydantic.field_serializer(*args, **kwargs)  # type: ignore

else:

    def field_validator(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        mode = kwargs.pop("mode", "after")
        return pydantic.validator(*args, **kwargs, pre=mode.lower() == "before", allow_reuse=True)

    def model_validator(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        mode = kwargs.pop("mode", "after")
        return pydantic.root_validator(
            *args, **kwargs, pre=mode.lower() == "before", allow_reuse=True
        )

    def field_serializer(*args: t.Any, **kwargs: t.Any) -> t.Callable[[t.Any], t.Any]:
        def _decorator(func: t.Callable[[t.Any], t.Any]) -> t.Callable[[t.Any], t.Any]:
            @wraps(func)
            def _wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
                return func(*args, **kwargs)

            return _wrapper

        return _decorator


def _expression_encoder(e: exp.Expression) -> str:
    return e.meta.get("sql") or e.sql(dialect=e.meta.get("dialect"))


AuditQueryTypes = t.Union[exp.Subqueryable, d.JinjaQuery]
ModelQueryTypes = t.Union[exp.Subqueryable, d.JinjaQuery, d.MacroFunc]


class PydanticModel(pydantic.BaseModel):
    if PYDANTIC_MAJOR_VERSION >= 2:

        model_config = pydantic.ConfigDict(  # type: ignore
            arbitrary_types_allowed=True,
            extra="forbid",  # type: ignore
            # Even though Pydantic v2 kept support for json_encoders, the functionality has been
            # crippled badly. Here we need to enumerate all different ways of how sqlglot expressions
            # show up in pydantic models.
            json_encoders={
                exp.Expression: _expression_encoder,
                exp.DataType: _expression_encoder,
                exp.Tuple: _expression_encoder,
                AuditQueryTypes: _expression_encoder,  # type: ignore
                ModelQueryTypes: _expression_encoder,  # type: ignore
            },
            protected_namespaces=(),
        )
    else:

        class Config:
            arbitrary_types_allowed = True
            extra = "forbid"
            json_encoders = {exp.Expression: _expression_encoder}
            underscore_attrs_are_private = True
            smart_union = True

    def dict(
        self,
        **kwargs: t.Any,
    ) -> t.Dict[str, t.Any]:
        kwargs.update(DEFAULT_ARGS)
        return super().model_dump(**kwargs) if PYDANTIC_MAJOR_VERSION >= 2 else super().dict(**kwargs)  # type: ignore

    def json(
        self,
        **kwargs: t.Any,
    ) -> str:
        kwargs.update(DEFAULT_ARGS)
        if PYDANTIC_MAJOR_VERSION >= 2:
            # Pydantic v2 doesn't support arbitrary arguments for json.dump().
            kwargs.pop("sort_keys", None)
            return super().model_dump_json(**kwargs)  # type: ignore
        return super().json(**kwargs)  # type: ignore

    def copy(self: "Model", **kwargs: t.Any) -> "Model":
        return (
            super().model_copy(**kwargs) if PYDANTIC_MAJOR_VERSION >= 2 else super().copy(**kwargs)  # type: ignore
        )

    @property
    def fields_set(self: "Model") -> t.Set[str]:
        return self.__pydantic_fields_set__ if PYDANTIC_MAJOR_VERSION >= 2 else self.__fields_set__  # type: ignore

    @classmethod
    def parse_obj(cls: t.Type["Model"], obj: t.Any) -> "Model":
        return (
            super().model_validate(obj) if PYDANTIC_MAJOR_VERSION >= 2 else super().parse_obj(obj)  # type: ignore
        )

    @classmethod
    def parse_raw(cls: t.Type["Model"], b: t.Union[str, bytes], **kwargs: t.Any) -> "Model":
        return (
            super().model_validate_json(b, **kwargs) if PYDANTIC_MAJOR_VERSION >= 2 else super().parse_raw(b, **kwargs)  # type: ignore
        )

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
    def all_field_infos(cls: t.Type["PydanticModel"]) -> t.Dict[str, FieldInfo]:
        return cls.model_fields if PYDANTIC_MAJOR_VERSION >= 2 else cls.__fields__  # type: ignore

    @classmethod
    def required_fields(cls: t.Type["PydanticModel"]) -> t.Set[str]:
        return cls._fields(lambda field: field.is_required() if PYDANTIC_MAJOR_VERSION >= 2 else field.required)  # type: ignore

    @classmethod
    def _fields(
        cls: t.Type["PydanticModel"],
        predicate: t.Callable[[t.Any], bool] = lambda _: True,
    ) -> t.Set[str]:
        return {
            field_info.alias if field_info.alias else field_name
            for field_name, field_info in cls.all_field_infos().items()  # type: ignore
            if predicate(field_info)
        }


def model_validator_v1_args(func: t.Callable[..., t.Any]) -> t.Callable[..., t.Any]:
    @wraps(func)
    def wrapper(cls: t.Type, values: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
        is_values_dict = isinstance(values, dict)
        values_dict = values if is_values_dict else values.__dict__
        result = func(cls, values_dict, *args, **kwargs)
        if is_values_dict:
            return result
        else:
            values.__dict__.update(result)
            return values

    return wrapper


def field_validator_v1_args(func: t.Callable[..., t.Any]) -> t.Callable[..., t.Any]:
    @wraps(func)
    def wrapper(cls: t.Type, v: t.Any, values: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
        values_dict = values if isinstance(values, dict) else values.data
        return func(cls, v, values_dict, *args, **kwargs)

    return wrapper


def validate_list_of_strings(v: t.Any) -> t.List[str]:
    if isinstance(v, exp.Identifier):
        return [v.name]
    if isinstance(v, (exp.Tuple, exp.Array)):
        return [e.name for e in v.expressions]
    return [i.name if isinstance(i, exp.Identifier) else str(i) for i in v]


def validate_string(v: t.Any) -> str:
    if isinstance(v, exp.Expression):
        return v.name
    return str(v)


def bool_validator(v: t.Any) -> bool:
    if isinstance(v, exp.Boolean):
        return v.this
    if isinstance(v, exp.Expression):
        return str_to_bool(v.name)
    return str_to_bool(str(v or ""))


def positive_int_validator(v: t.Any) -> int:
    if isinstance(v, exp.Expression) and v.is_int:
        v = int(v.name)
    if not isinstance(v, int):
        raise ValueError(f"Invalid num {v}. Value must be an integer value")
    if v <= 0:
        raise ValueError(f"Invalid num {v}. Value must be a positive integer")
    return v


if t.TYPE_CHECKING:
    SQLGlotListOfStrings = t.List[str]
    SQLGlotString = str
    SQLGlotBool = bool
    SQLGlotPositiveInt = int
elif PYDANTIC_MAJOR_VERSION >= 2:
    from pydantic.functional_validators import BeforeValidator  # type: ignore

    SQLGlotListOfStrings = Annotated[t.List[str], BeforeValidator(validate_list_of_strings)]
    SQLGlotString = Annotated[str, BeforeValidator(validate_string)]
    SQLGlotBool = Annotated[bool, BeforeValidator(bool_validator)]
    SQLGlotPositiveInt = Annotated[int, BeforeValidator(positive_int_validator)]
else:

    T = t.TypeVar("T")

    class PydanticTypeProxy(t.Generic[T]):
        validate: t.Callable[[t.Any], T]

        @classmethod
        def __get_validators__(cls) -> t.Iterator[t.Callable[[t.Any], T]]:
            yield cls.validate

    class SQLGlotListOfStrings(PydanticTypeProxy[t.List[str]]):
        validate = validate_list_of_strings

    class SQLGlotString(PydanticTypeProxy[str]):
        validate = validate_string

    class SQLGlotBool(PydanticTypeProxy[bool]):
        validate = bool_validator

    class SQLGlotPositiveInt(PydanticTypeProxy[int]):
        validate = positive_int_validator
