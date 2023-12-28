from __future__ import annotations

import functools
import inspect
import types
import typing as t
from enum import Enum

from pydantic import Field
from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.utils.errors import UnsupportedCatalogOperationError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import Query


class DataObjectType(str, Enum):
    UNKNOWN = "unknown"
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"

    @property
    def is_unknown(self) -> bool:
        return self == DataObjectType.UNKNOWN

    @property
    def is_table(self) -> bool:
        return self == DataObjectType.TABLE

    @property
    def is_view(self) -> bool:
        return self == DataObjectType.VIEW

    @property
    def is_materialized_view(self) -> bool:
        return self == DataObjectType.MATERIALIZED_VIEW

    @classmethod
    def from_str(cls, s: str) -> DataObjectType:
        s = s.lower()
        if s == "table":
            return DataObjectType.TABLE
        if s == "view":
            return DataObjectType.VIEW
        if s == "materialized_view":
            return DataObjectType.MATERIALIZED_VIEW
        return DataObjectType.UNKNOWN


class DataObject(PydanticModel):
    catalog: t.Optional[str] = None
    schema_name: str = Field(alias="schema")
    name: str
    type: DataObjectType


def _get_args_pos_and_kwarg_name(
    func: t.Callable,
) -> t.Optional[t.Tuple[str, int, str]]:
    spec = inspect.getfullargspec(func)
    for i, name in enumerate(spec.args):
        obj_type = spec.annotations.get(name)
        if obj_type == "SchemaName":
            return name, i, obj_type
        if obj_type == "TableName":
            return name, i, obj_type
    return None


class CatalogSupport(Enum):
    UNSUPPORTED = 1
    SINGLE_CATALOG_ONLY = 2
    REQUIRES_SET_CATALOG = 3
    FULL_SUPPORT = 4

    @property
    def is_unsupported(self) -> bool:
        return self == CatalogSupport.UNSUPPORTED

    @property
    def is_single_catalog_only(self) -> bool:
        return self == CatalogSupport.SINGLE_CATALOG_ONLY

    @property
    def is_requires_set_catalog(self) -> bool:
        return self == CatalogSupport.REQUIRES_SET_CATALOG

    @property
    def is_full_support(self) -> bool:
        return self == CatalogSupport.FULL_SUPPORT

    @property
    def is_multi_catalog_supported(self) -> bool:
        return self.is_requires_set_catalog or self.is_full_support


def set_catalog(
    *,
    override: t.Optional[CatalogSupport] = None,
) -> t.Callable:
    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
            # Need to convert args to list in order to later do assignment to the object
            list_args = list(args)
            engine_adapter = list_args[0]
            catalog_support = override or engine_adapter.CATALOG_SUPPORT
            # If there is full catalog support then we have nothing to do
            if catalog_support.is_full_support:
                return func(*list_args, **kwargs)

            # Get the field value and the container which it came from so we can update it later
            location = _get_args_pos_and_kwarg_name(func)
            if location is None:
                return func(*list_args, **kwargs)
            name, pos, obj_type = location
            obj, container, key = t.cast(
                t.Tuple[t.Union[str, exp.Table], t.Union[t.Dict, t.List], t.Union[int, str]],
                (kwargs.get(name), kwargs, name)
                if kwargs.get(name)
                else (list_args[pos], list_args, pos),
            )
            to_expression_func = t.cast(
                t.Callable[[t.Union[str, exp.Table]], exp.Table],
                exp.to_table if obj_type == "TableName" else to_schema,
            )
            expression = to_expression_func(obj.copy() if isinstance(obj, exp.Table) else obj)
            catalog_name = expression.catalog
            if not catalog_name:
                return func(*list_args, **kwargs)
            # If we have a catalog and this engine doesn't support catalogs then we need to error
            if catalog_support.is_unsupported:
                raise UnsupportedCatalogOperationError(
                    f"{engine_adapter.dialect} does not support catalogs and a catalog was provided: {catalog_name}"
                )
            # Remove the catalog name from the argument so the engine adapter doesn't try to use it
            expression.set("catalog", None)
            container[key] = expression  # type: ignore
            if catalog_support.is_single_catalog_only:
                if catalog_name != engine_adapter._default_catalog:
                    raise UnsupportedCatalogOperationError(
                        f"{engine_adapter.dialect} requires that all catalog operations be against a single catalog: {engine_adapter._default_catalog}"
                    )
                return func(*list_args, **kwargs)
            # Set the catalog name on the engine adapter if needed
            current_catalog = engine_adapter.get_current_catalog()
            if catalog_name != current_catalog:
                engine_adapter.set_current_catalog(catalog_name)
                resp = func(*list_args, **kwargs)
                engine_adapter.set_current_catalog(current_catalog)
            else:
                resp = func(*list_args, **kwargs)
            return resp

        return wrapper

    return decorator


class InsertOverwriteStrategy(Enum):
    DELETE_INSERT = 1
    INSERT_OVERWRITE = 2
    # Note: Replace where on Databricks requires that `spark.sql.sources.partitionOverwriteMode` be set to `static`
    REPLACE_WHERE = 3
    INTO_IS_OVERWRITE = 4

    @property
    def is_delete_insert(self) -> bool:
        return self == InsertOverwriteStrategy.DELETE_INSERT

    @property
    def is_insert_overwrite(self) -> bool:
        return self == InsertOverwriteStrategy.INSERT_OVERWRITE

    @property
    def is_replace_where(self) -> bool:
        return self == InsertOverwriteStrategy.REPLACE_WHERE

    @property
    def is_into_is_overwrite(self) -> bool:
        return self == InsertOverwriteStrategy.INTO_IS_OVERWRITE

    @property
    def requires_condition(self) -> bool:
        return self.is_replace_where or self.is_delete_insert


class SourceQuery:
    def __init__(
        self,
        query_factory: t.Callable[[], Query],
        cleanup_func: t.Optional[t.Callable[[], None]] = None,
        **kwargs: t.Any,
    ) -> None:
        self.query_factory = query_factory
        self.cleanup_func = cleanup_func

    def __enter__(self) -> Query:
        return self.query_factory()

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[types.TracebackType],
    ) -> t.Optional[bool]:
        if self.cleanup_func:
            self.cleanup_func()
        return None
