from __future__ import annotations

import abc
import logging
import typing as t

import pandas as pd
from dbt.contracts.relation import Policy
from sqlglot import exp, parse_one
from sqlglot.helper import seq_get

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import Snapshot, to_table_mapping
from sqlmesh.utils.errors import ConfigError, ParsetimeAdapterCallError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroReference

if t.TYPE_CHECKING:
    import agate
    from dbt.adapters.base import BaseRelation
    from dbt.adapters.base.column import Column
    from dbt.adapters.base.impl import AdapterResponse


logger = logging.getLogger(__name__)


class BaseAdapter(abc.ABC):
    def __init__(
        self,
        jinja_macros: JinjaMacroRegistry,
        jinja_globals: t.Optional[t.Dict[str, t.Any]] = None,
        dialect: t.Optional[str] = None,
    ):
        self.jinja_macros = jinja_macros
        self.jinja_globals = jinja_globals.copy() if jinja_globals else {}
        self.jinja_globals["adapter"] = self
        self.dialect = dialect

    @abc.abstractmethod
    def get_relation(self, database: str, schema: str, identifier: str) -> t.Optional[BaseRelation]:
        """Returns a single relation that matches the provided path."""

    @abc.abstractmethod
    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        """Gets all relations in a given schema and optionally database.

        TODO: Add caching functionality to avoid repeat visits to DB
        """

    @abc.abstractmethod
    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        """Using the engine adapter, gets all the relations that match the given schema grain relation."""

    @abc.abstractmethod
    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        """Returns the columns for a given table grained relation."""

    @abc.abstractmethod
    def get_missing_columns(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> t.List[Column]:
        """Returns the columns in from_relation missing from to_relation."""

    @abc.abstractmethod
    def create_schema(self, relation: BaseRelation) -> None:
        """Creates a schema in the target database."""

    @abc.abstractmethod
    def drop_schema(self, relation: BaseRelation) -> None:
        """Drops a schema in the target database."""

    @abc.abstractmethod
    def drop_relation(self, relation: BaseRelation) -> None:
        """Drops a relation (table) in the target database."""

    @abc.abstractmethod
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> t.Tuple[AdapterResponse, agate.Table]:
        """Executes the given SQL statement and returns the results as an agate table."""

    @abc.abstractmethod
    def resolve_schema(self, relation: BaseRelation) -> t.Optional[str]:
        """Resolves the relation's schema to its physical schema."""

    @abc.abstractmethod
    def resolve_identifier(self, relation: BaseRelation) -> t.Optional[str]:
        """Resolves the relation's schema to its physical identifier."""

    def quote(self, identifier: str) -> str:
        """Returns a quoted identifier."""
        return exp.to_column(identifier).sql(dialect=self.dialect, identify=True)

    def dispatch(self, name: str, package: t.Optional[str] = None) -> t.Callable:
        """Returns a dialect-specific version of a macro with the given name."""
        target_type = self.jinja_globals["target"]["type"]
        references_to_try = [
            MacroReference(package=f"{package}_{target_type}", name=f"{target_type}__{name}"),
            MacroReference(package=package, name=f"{target_type}__{name}"),
            MacroReference(package=package, name=f"default__{name}"),
        ]

        for reference in references_to_try:
            macro_callable = self.jinja_macros.build_macro(reference, **self.jinja_globals)
            if macro_callable is not None:
                return macro_callable

        raise ConfigError(f"Macro '{name}', package '{package}' was not found.")


class ParsetimeAdapter(BaseAdapter):
    def get_relation(self, database: str, schema: str, identifier: str) -> t.Optional[BaseRelation]:
        self._raise_parsetime_adapter_call_error("get relation")
        raise

    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        self._raise_parsetime_adapter_call_error("list relation")
        raise

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        self._raise_parsetime_adapter_call_error("list relation")
        raise

    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        self._raise_parsetime_adapter_call_error("get columns")
        raise

    def get_missing_columns(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> t.List[Column]:
        self._raise_parsetime_adapter_call_error("get missing columns")
        raise

    def create_schema(self, relation: BaseRelation) -> None:
        self._raise_parsetime_adapter_call_error("create schema")

    def drop_schema(self, relation: BaseRelation) -> None:
        self._raise_parsetime_adapter_call_error("drop schema")

    def drop_relation(self, relation: BaseRelation) -> None:
        self._raise_parsetime_adapter_call_error("drop relation")

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> t.Tuple[AdapterResponse, agate.Table]:
        self._raise_parsetime_adapter_call_error("execute SQL")
        raise

    def resolve_schema(self, relation: BaseRelation) -> t.Optional[str]:
        return relation.schema

    def resolve_identifier(self, relation: BaseRelation) -> t.Optional[str]:
        return relation.identifier

    @staticmethod
    def _raise_parsetime_adapter_call_error(action: str) -> None:
        raise ParsetimeAdapterCallError(f"Can't {action} at parse time.")


class RuntimeAdapter(BaseAdapter):
    def __init__(
        self,
        engine_adapter: EngineAdapter,
        jinja_macros: JinjaMacroRegistry,
        jinja_globals: t.Optional[t.Dict[str, t.Any]] = None,
        relation_type: t.Optional[t.Type[BaseRelation]] = None,
        quote_policy: t.Optional[Policy] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        is_dev: bool = False,
    ):
        from dbt.adapters.base import BaseRelation
        from dbt.adapters.base.relation import Policy

        super().__init__(jinja_macros, jinja_globals=jinja_globals, dialect=engine_adapter.dialect)

        table_mapping = table_mapping or {}

        self.engine_adapter = engine_adapter
        self.relation_type = relation_type or BaseRelation
        self.quote_policy = quote_policy or Policy()
        self.table_mapping = {
            **to_table_mapping((snapshots or {}).values(), is_dev),
            **table_mapping,
        }

    def get_relation(
        self, database: t.Optional[str], schema: str, identifier: str
    ) -> t.Optional[BaseRelation]:
        mapped_table = self._map_table_name(database, schema, identifier)
        schema, identifier = mapped_table.db, mapped_table.name

        relations_list = self.list_relations(database, schema)
        matching_relations = [
            r
            for r in relations_list
            if r.identifier == identifier
            and r.schema == schema
            and (r.database == database or database is None)
        ]
        return seq_get(matching_relations, 0)

    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        reference_relation = self.relation_type.create(
            database=database,
            schema=schema,
        )
        return self.list_relations_without_caching(reference_relation)

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        from dbt.contracts.relation import RelationType

        assert schema_relation.schema is not None
        data_objects = self.engine_adapter._get_data_objects(
            schema_name=schema_relation.schema, catalog_name=schema_relation.database
        )
        relations = [
            self.relation_type.create(
                database=do.catalog,
                schema=do.schema_name,
                identifier=do.name,
                quote_policy=self.quote_policy,
                # DBT relation types aren't snake case and instead just one word without spaces so we remove underscores
                type=RelationType.External
                if do.type.is_unknown
                else RelationType(do.type.lower().replace("_", "")),
            )
            for do in data_objects
        ]
        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        from dbt.adapters.base.column import Column

        mapped_table = self._map_table_name(relation.database, relation.schema, relation.identifier)

        return [
            Column.from_description(
                name=name, raw_data_type=dtype.sql(dialect=self.engine_adapter.dialect)
            )
            for name, dtype in self.engine_adapter.columns(table_name=mapped_table).items()
        ]

    def get_missing_columns(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> t.List[Column]:
        target_columns = {col.name for col in self.get_columns_in_relation(to_relation)}

        return [
            col
            for col in self.get_columns_in_relation(from_relation)
            if col.name not in target_columns
        ]

    def create_schema(self, relation: BaseRelation) -> None:
        if relation.schema is not None:
            self.engine_adapter.create_schema(relation.schema)

    def drop_schema(self, relation: BaseRelation) -> None:
        if relation.schema is not None:
            self.engine_adapter.drop_schema(relation.schema)

    def drop_relation(self, relation: BaseRelation) -> None:
        if relation.schema is not None and relation.identifier is not None:
            self.engine_adapter.drop_table(f"{relation.schema}.{relation.identifier}")

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> t.Tuple[AdapterResponse, agate.Table]:
        from dbt.adapters.base.impl import AdapterResponse
        from dbt.clients.agate_helper import empty_table

        from sqlmesh.dbt.util import pandas_to_agate

        # mypy bug: https://github.com/python/mypy/issues/10740
        exec_func: t.Callable[[exp.Expression], None | pd.DataFrame] = (
            self.engine_adapter.fetchdf if fetch else self.engine_adapter.execute  # type: ignore
        )

        expression = parse_one(sql, read=self.engine_adapter.dialect)
        expression = exp.replace_tables(expression, self.table_mapping, copy=False)

        if auto_begin:
            # TODO: This could be a bug. I think dbt leaves the transaction open while we close immediately.
            with self.engine_adapter.transaction():
                resp = exec_func(expression)
        else:
            resp = exec_func(expression)

        # TODO: Properly fill in adapter response
        if fetch:
            assert isinstance(resp, pd.DataFrame)
            return AdapterResponse("Success"), pandas_to_agate(resp)
        return AdapterResponse("Success"), empty_table()

    def resolve_schema(self, relation: BaseRelation) -> t.Optional[str]:
        schema = self._map_table_name(relation.database, relation.schema, relation.identifier).db
        if not schema:
            return None
        return schema

    def resolve_identifier(self, relation: BaseRelation) -> t.Optional[str]:
        identifier = self._map_table_name(
            relation.database, relation.schema, relation.identifier
        ).name
        if not identifier:
            return None
        return identifier

    def _map_table_name(
        self, database: t.Optional[str], schema: t.Optional[str], identifier: t.Optional[str]
    ) -> exp.Table:
        name = ".".join(p for p in (database, schema, identifier) if p is not None)
        if name not in self.table_mapping:
            return exp.to_table(name, dialect=self.engine_adapter.dialect)

        physical_table_name = self.table_mapping[name]
        logger.debug("Resolved ref '%s' to snapshot table '%s'", name, physical_table_name)

        return exp.to_table(physical_table_name, dialect=self.engine_adapter.dialect)
