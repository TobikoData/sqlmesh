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
    from sqlmesh.core.engine_adapter.base import EngineAdapter


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


class CommentCreationTable(Enum):
    """
    Enum for SQL engine TABLE comment support.

    UNSUPPORTED = no comments at all
    IN_SCHEMA_DEF_CTAS = comments can be registered in CREATE schema definitions, including CTAS calls
    IN_SCHEMA_DEF_NO_CTAS = comments can be registered in CREATE schema definitions, excluding CTAS calls
    COMMENT_COMMAND_ONLY = comments can only be registered via a post-creation command like `COMMENT` or `ALTER`
    """

    UNSUPPORTED = 1
    IN_SCHEMA_DEF_CTAS = 2
    IN_SCHEMA_DEF_NO_CTAS = 3
    COMMENT_COMMAND_ONLY = 4

    @property
    def is_unsupported(self) -> bool:
        return self == CommentCreationTable.UNSUPPORTED

    @property
    def is_in_schema_def_ctas(self) -> bool:
        return self == CommentCreationTable.IN_SCHEMA_DEF_CTAS

    @property
    def is_in_schema_def_no_ctas(self) -> bool:
        return self == CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS

    @property
    def is_comment_command_only(self) -> bool:
        return self == CommentCreationTable.COMMENT_COMMAND_ONLY

    @property
    def is_supported(self) -> bool:
        return self != CommentCreationTable.UNSUPPORTED

    @property
    def supports_schema_def(self) -> bool:
        return self in (
            CommentCreationTable.IN_SCHEMA_DEF_CTAS,
            CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS,
        )


class CommentCreationView(Enum):
    """
    Enum for SQL engine VIEW comment support.

    UNSUPPORTED = no comments at all
    IN_SCHEMA_DEF_AND_COMMANDS = all comments can be registered in CREATE VIEW schema definitions
                                   and in post-creation commands
    IN_SCHEMA_DEF_NO_COMMANDS = all comments can be registered in CREATE VIEW schema definitions,
                                  but not in post-creation commands
    IN_SCHEMA_DEF_NO_COLUMN_COMMAND = all comments can be registered in CREATE VIEW schema definitions,
                                        view comments can be registered in post-creation commands,
                                        column comments cannot be registered in post-creation commands
    COMMENT_COMMAND_ONLY = comments can only be registered via a post-creation command like `COMMENT` or `ALTER`
    """

    UNSUPPORTED = 1
    IN_SCHEMA_DEF_AND_COMMANDS = 2
    IN_SCHEMA_DEF_NO_COMMANDS = 3
    IN_SCHEMA_DEF_NO_COLUMN_COMMAND = 4
    COMMENT_COMMAND_ONLY = 5

    @property
    def is_unsupported(self) -> bool:
        return self == CommentCreationView.UNSUPPORTED

    @property
    def is_in_schema_def_and_commands(self) -> bool:
        return self == CommentCreationView.IN_SCHEMA_DEF_AND_COMMANDS

    @property
    def is_in_schema_def_no_commands(self) -> bool:
        return self == CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS

    @property
    def is_in_schema_def_no_column_command(self) -> bool:
        return self == CommentCreationView.IN_SCHEMA_DEF_NO_COLUMN_COMMAND

    @property
    def is_comment_command_only(self) -> bool:
        return self == CommentCreationView.COMMENT_COMMAND_ONLY

    @property
    def is_supported(self) -> bool:
        return self != CommentCreationView.UNSUPPORTED

    @property
    def supports_schema_def(self) -> bool:
        return self in (
            CommentCreationView.IN_SCHEMA_DEF_AND_COMMANDS,
            CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS,
            CommentCreationView.IN_SCHEMA_DEF_NO_COLUMN_COMMAND,
        )

    @property
    def supports_column_comment_commands(self) -> bool:
        return self in (
            CommentCreationView.IN_SCHEMA_DEF_AND_COMMANDS,
            CommentCreationView.COMMENT_COMMAND_ONLY,
        )


class DataObject(PydanticModel):
    catalog: t.Optional[str] = None
    schema_name: str = Field(alias="schema")
    name: str
    type: DataObjectType


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


class SourceQuery:
    def __init__(
        self,
        query_factory: t.Callable[[], Query],
        cleanup_func: t.Optional[t.Callable[[], None]] = None,
        transforms: t.Optional[t.List[t.Callable[[Query], Query]]] = None,
        **kwargs: t.Any,
    ) -> None:
        self.query_factory = query_factory
        self.cleanup_func = cleanup_func
        self._transforms = transforms or []

    def add_transform(self, transform: t.Callable[[Query], Query]) -> None:
        self._transforms.append(transform)

    def __enter__(self) -> Query:
        query = self.query_factory()
        for transform in self._transforms:
            query = query.transform(transform)
        return query

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[types.TracebackType],
    ) -> t.Optional[bool]:
        if self.cleanup_func:
            self.cleanup_func()
        return None


def set_catalog(override_mapping: t.Optional[t.Dict[str, CatalogSupport]] = None) -> t.Callable:
    def set_catalog_decorator(
        func: t.Callable,
        target_name: str,
        target_pos: int,
        target_type: str,
        override: t.Optional[CatalogSupport] = None,
    ) -> t.Callable:
        @functools.wraps(func)
        def internal_wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
            # Need to convert args to list in order to later do assignment to the object
            list_args = list(args)
            engine_adapter = list_args[0]
            catalog_support = override or engine_adapter.CATALOG_SUPPORT
            # If there is full catalog support then we have nothing to do
            if catalog_support.is_full_support:
                return func(*list_args, **kwargs)

            obj, container, key = t.cast(
                t.Tuple[t.Union[str, exp.Table], t.Union[t.Dict, t.List], t.Union[int, str]],
                (
                    (kwargs.get(target_name), kwargs, target_name)
                    if kwargs.get(target_name)
                    else (list_args[target_pos], list_args, target_pos)
                ),
            )
            to_expression_func = t.cast(
                t.Callable[[t.Union[str, exp.Table]], exp.Table],
                exp.to_table if target_type == "TableName" else to_schema,
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

        return internal_wrapper

    inclusion_list = {
        "_get_data_objects",
    }

    # Exclude this to avoid a circular dependency from inspecting the classproperty
    exclusion_list = {
        "can_access_spark_session",
    }

    override_mapping = override_mapping or {}

    def wrapper(cls: t.Type[EngineAdapter]) -> t.Callable:
        for name in dir(cls):
            if name in exclusion_list or (name.startswith("_") and name not in inclusion_list):
                continue
            m = getattr(cls, name)
            if inspect.isfunction(m):
                spec = inspect.getfullargspec(m)
                for i, obj_name in enumerate(spec.args):
                    obj_type = spec.annotations.get(obj_name)
                    if obj_type not in {"SchemaName", "TableName"}:
                        continue
                    setattr(
                        cls,
                        name,
                        set_catalog_decorator(
                            m,
                            target_name=obj_name,
                            target_pos=i,
                            target_type=obj_type,
                            override=override_mapping.get(name),
                        ),
                    )
        return cls

    return wrapper
