from __future__ import annotations

import abc
import logging
import typing as t

from sqlglot import exp, parse_one

from sqlmesh.core.dialect import normalize_and_quote, normalize_model_name, schema_
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import DeployabilityIndex, Snapshot, to_table_mapping
from sqlmesh.utils.errors import ConfigError, ParsetimeAdapterCallError
from sqlmesh.utils.jinja import JinjaMacroRegistry

if t.TYPE_CHECKING:
    import agate
    import pandas as pd
    from dbt.adapters.base import BaseRelation
    from dbt.adapters.base.column import Column
    from dbt.adapters.base.impl import AdapterResponse
    from sqlmesh.dbt.relation import Policy


logger = logging.getLogger(__name__)


class BaseAdapter(abc.ABC):
    def __init__(
        self,
        jinja_macros: JinjaMacroRegistry,
        jinja_globals: t.Optional[t.Dict[str, t.Any]] = None,
        project_dialect: t.Optional[str] = None,
        quote_policy: t.Optional[Policy] = None,
    ):
        from dbt.adapters.base.relation import Policy

        self.jinja_macros = jinja_macros
        self.jinja_globals = jinja_globals.copy() if jinja_globals else {}
        self.jinja_globals["adapter"] = self
        self.project_dialect = project_dialect
        self.jinja_globals["dialect"] = (
            project_dialect  # so the dialect is available in the jinja env created by self.dispatch()
        )
        self.quote_policy = quote_policy or Policy()

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
        return exp.to_column(identifier).sql(dialect=self.project_dialect, identify=True)

    def quote_as_configured(self, value: str, component_type: str) -> str:
        """Returns the value quoted according to the quote policy."""
        return self.quote(value) if getattr(self.quote_policy, component_type, False) else value

    def dispatch(self, name: str, package: t.Optional[str] = None) -> t.Callable:
        """Returns a dialect-specific version of a macro with the given name."""
        target_type = self.jinja_globals["target"]["type"]
        macro_suffix = f"__{name}"

        def _relevance(package_name_pair: t.Tuple[t.Optional[str], str]) -> t.Tuple[int, int]:
            """Lower scores more relevant."""
            macro_package, macro_name = package_name_pair

            package_score = 0 if macro_package == package else 1
            name_score = 1

            if macro_name.startswith("default"):
                name_score = 2
            elif macro_name.startswith(target_type):
                name_score = 0

            return name_score, package_score

        jinja_env = self.jinja_macros.build_environment(**self.jinja_globals).globals
        packages_to_check: t.List[t.Optional[str]] = [
            package,
            *(k for k in jinja_env if k.startswith("dbt")),
        ]
        candidates = {}
        for macro_package in packages_to_check:
            macros = jinja_env.get(macro_package, {}) if macro_package else jinja_env
            if not isinstance(macros, dict):
                continue
            candidates.update(
                {
                    (macro_package, macro_name): macro_callable
                    for macro_name, macro_callable in macros.items()
                    if macro_name.endswith(macro_suffix)
                }
            )

        if candidates:
            sorted_candidates = sorted(candidates, key=_relevance)
            return candidates[sorted_candidates[0]]

        raise ConfigError(f"Macro '{name}', package '{package}' was not found.")

    def type(self) -> str:
        return self.project_dialect or ""

    def compare_dbr_version(self, major: int, minor: int) -> int:
        # This method is specific to the Databricks dbt adapter implementation and is used in some macros.
        # Always return -1 to fallback to Spark macro implementations.
        return -1


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
        project_dialect: t.Optional[str] = None,
    ):
        from dbt.adapters.base import BaseRelation
        from dbt.adapters.base.column import Column

        super().__init__(
            jinja_macros,
            jinja_globals=jinja_globals,
            project_dialect=project_dialect or engine_adapter.dialect,
            quote_policy=quote_policy,
        )

        table_mapping = table_mapping or {}

        self.engine_adapter = engine_adapter
        self.relation_type = relation_type or BaseRelation
        self.column_type = column_type or Column
        self.table_mapping = {
            **to_table_mapping((snapshots or {}).values(), deployability_index),
            **table_mapping,
        }

    def get_relation(
        self, database: t.Optional[str], schema: str, identifier: str
    ) -> t.Optional[BaseRelation]:
        target_table = exp.table_(identifier, db=schema, catalog=database)
        # Normalize before converting to a relation; otherwise, it will be too late,
        # as quotes will have already been applied.
        target_table = self._normalize(target_table)
        return self.load_relation(self._table_to_relation(target_table))

    def load_relation(self, relation: BaseRelation) -> t.Optional[BaseRelation]:
        mapped_table = self._map_table_name(self._normalize(self._relation_to_table(relation)))
        if not self.engine_adapter.table_exists(mapped_table):
            return None

        return self._table_to_relation(mapped_table)

    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        target_schema = schema_(schema, catalog=database)
        # Normalize before converting to a relation; otherwise, it will be too late,
        # as quotes will have already been applied.
        target_schema = self._normalize(target_schema)
        return self.list_relations_without_caching(self._table_to_relation(target_schema))

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        from sqlmesh.dbt.relation import RelationType

        schema = self._normalize(self._schema(schema_relation))

        relations = [
            self.relation_type.create(
                database=do.catalog,
                schema=do.schema_name,
                identifier=do.name,
                quote_policy=self.quote_policy,
                # DBT relation types aren't snake case and instead just one word without spaces so we remove underscores
                type=(
                    RelationType.External
                    if do.type.is_unknown
                    else RelationType(do.type.lower().replace("_", ""))
                ),
            )
            for do in self.engine_adapter.get_data_objects(schema)
        ]
        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        from dbt.adapters.base.column import Column

        mapped_table = self._map_table_name(self._normalize(self._relation_to_table(relation)))
        return [
            Column.from_description(
                name=name, raw_data_type=dtype.sql(dialect=self.project_dialect)
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
        import pandas as pd
        from dbt.adapters.base.impl import AdapterResponse

        from sqlmesh.dbt.util import pandas_to_agate, empty_table

        # mypy bug: https://github.com/python/mypy/issues/10740
        exec_func: t.Callable[..., None | pd.DataFrame] = (
            self.engine_adapter.fetchdf if fetch else self.engine_adapter.execute  # type: ignore
        )

        expression = parse_one(sql, read=self.project_dialect)
        with normalize_and_quote(
            expression, t.cast(str, self.project_dialect), self.engine_adapter.default_catalog
        ) as expression:
            expression = exp.replace_tables(
                expression, self.table_mapping, dialect=self.project_dialect, copy=False
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
        # Use the default dialect since this is the dialect used to normalize and quote keys in the
        # mapping table.
        name = table.sql(identify=True)
        physical_table_name = self.table_mapping.get(name)
        if not physical_table_name:
            return table

        logger.debug("Resolved ref '%s' to snapshot table '%s'", name, physical_table_name)

        return exp.to_table(physical_table_name, dialect=self.project_dialect)

    def _relation_to_table(self, relation: BaseRelation) -> exp.Table:
        return exp.to_table(relation.render(), dialect=self.project_dialect)

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
            input_table, self.engine_adapter.default_catalog, self.project_dialect
        )
        normalized_table = exp.to_table(normalized_name)
        if not input_table.this:
            normalized_table.set("catalog", normalized_table.args.get("db"))
            normalized_table.set("db", normalized_table.this)
            normalized_table.set("this", None)
        return normalized_table
