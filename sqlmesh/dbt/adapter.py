from __future__ import annotations

import abc
import logging
import typing as t

import pandas as pd
from dbt.contracts.relation import Policy
from sqlglot import exp, parse_one

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.renderer import _normalize_and_quote
from sqlmesh.core.snapshot import DeployabilityIndex, Snapshot, to_table_mapping
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
    def load_relation(self, relation: BaseRelation) -> t.Optional[BaseRelation]:
        """Returns a single relation that matches the provided relation if present."""

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

    def load_relation(self, relation: BaseRelation) -> t.Optional[BaseRelation]:
        self._raise_parsetime_adapter_call_error("load relation")
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
        column_type: t.Optional[t.Type[Column]] = None,
        quote_policy: t.Optional[Policy] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
    ):
        from dbt.adapters.base import BaseRelation
        from dbt.adapters.base.column import Column
        from dbt.adapters.base.relation import Policy

        super().__init__(jinja_macros, jinja_globals=jinja_globals, dialect=engine_adapter.dialect)

        table_mapping = table_mapping or {}

        self.engine_adapter = engine_adapter
        self.relation_type = relation_type or BaseRelation
        self.column_type = column_type or Column
        self.quote_policy = quote_policy or Policy()
        self.table_mapping = {
            **to_table_mapping((snapshots or {}).values(), deployability_index),
            **table_mapping,
        }

    def get_relation(
        self, database: t.Optional[str], schema: str, identifier: str
    ) -> t.Optional[BaseRelation]:
        return self.load_relation(
            self.relation_type.create(
                database=database,
                schema=schema,
                identifier=identifier,
                quote_policy=self.quote_policy,
            )
        )

    def load_relation(self, relation: BaseRelation) -> t.Optional[BaseRelation]:
        mapped_table = self._map_table_name(self._normalize(self._relation_to_table(relation)))
        if not self.engine_adapter.table_exists(mapped_table):
            return None

        return self._table_to_relation(mapped_table)

    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        reference_relation = self.relation_type.create(
            database=database, schema=schema, quote_policy=self.quote_policy
        )
        return self.list_relations_without_caching(reference_relation)

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        from dbt.contracts.relation import RelationType

        schema = self._normalize(self._schema(schema_relation))

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
            for do in self.engine_adapter._get_data_objects(schema)
        ]
        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        from dbt.adapters.base.column import Column

        mapped_table = self._map_table_name(self._normalize(self._relation_to_table(relation)))
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
            self.engine_adapter.create_schema(self._normalize(self._schema(relation)))

    def drop_schema(self, relation: BaseRelation) -> None:
        if relation.schema is not None:
            self.engine_adapter.drop_schema(self._normalize(self._schema(relation)))

    def drop_relation(self, relation: BaseRelation) -> None:
        if relation.schema is not None and relation.identifier is not None:
            self.engine_adapter.drop_table(self._normalize(self._relation_to_table(relation)))

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> t.Tuple[AdapterResponse, agate.Table]:
        from dbt.adapters.base.impl import AdapterResponse
        from dbt.clients.agate_helper import empty_table

        from sqlmesh.dbt.util import pandas_to_agate

        # mypy bug: https://github.com/python/mypy/issues/10740
        exec_func: t.Callable[..., None | pd.DataFrame] = (
            self.engine_adapter.fetchdf if fetch else self.engine_adapter.execute  # type: ignore
        )

        expression = parse_one(sql, read=self.engine_adapter.dialect)
        with _normalize_and_quote(
            expression, self.engine_adapter.dialect, self.engine_adapter.default_catalog
        ) as expression:
            expression = exp.replace_tables(
                expression, self.table_mapping, dialect=self.dialect, copy=False
            )

        if auto_begin:
            # TODO: This could be a bug. I think dbt leaves the transaction open while we close immediately.
            with self.engine_adapter.transaction():
                resp = exec_func(expression, quote_identifiers=False)
        else:
            resp = exec_func(expression, quote_identifiers=False)

        # TODO: Properly fill in adapter response
        if fetch:
            assert isinstance(resp, pd.DataFrame)
            return AdapterResponse("Success"), pandas_to_agate(resp)
        return AdapterResponse("Success"), empty_table()

    def resolve_schema(self, relation: BaseRelation) -> t.Optional[str]:
        schema = self._map_table_name(self._normalize(self._relation_to_table(relation))).db
        return schema if schema else None

    def resolve_identifier(self, relation: BaseRelation) -> t.Optional[str]:
        identifier = self._map_table_name(self._normalize(self._relation_to_table(relation))).name
        return identifier if identifier else None

    def _map_table_name(self, table: exp.Table) -> exp.Table:
        name = table.sql()
        physical_table_name = self.table_mapping.get(name)
        if not physical_table_name:
            return table

        logger.debug("Resolved ref '%s' to snapshot table '%s'", name, physical_table_name)

        return exp.to_table(physical_table_name, dialect=self.engine_adapter.dialect)

    def _relation_to_table(self, relation: BaseRelation) -> exp.Table:
        table = exp.to_table(relation.render(), dialect=self.engine_adapter.dialect)
        return exp.to_table(relation.render(), dialect=self.engine_adapter.dialect)

    def _table_to_relation(self, table: exp.Table) -> BaseRelation:
        return self.relation_type.create(
            database=table.catalog or None,
            schema=table.db,
            identifier=table.name,
            quote_policy=self.quote_policy,
        )

    def _schema(self, schema_relation: BaseRelation) -> exp.Table:
        assert schema_relation.schema is not None
        return exp.Table(
            this=None,
            db=exp.to_identifier(schema_relation.schema, quoted=self.quote_policy.schema),
            catalog=exp.to_identifier(schema_relation.database, quoted=self.quote_policy.database),
        )

    def _normalize(self, input_table: exp.Table) -> exp.Table:
        normalized_name = normalize_model_name(
            input_table, self.engine_adapter.default_catalog, self.engine_adapter.dialect
        )
        normalized_table = exp.to_table(normalized_name)
        if not input_table.this:
            normalized_table.set("catalog", normalized_table.args.get("db"))
            normalized_table.set("db", normalized_table.this)
            normalized_table.set("this", None)
        return normalized_table
