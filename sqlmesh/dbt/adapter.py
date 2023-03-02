from __future__ import annotations

import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core.engine_adapter import EngineAdapter, TransactionType
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroReference

if t.TYPE_CHECKING:
    import agate
    from dbt.adapters.base import BaseRelation
    from dbt.adapters.base.column import Column
    from dbt.adapters.base.impl import AdapterResponse


class Adapter:
    def __init__(
        self,
        engine_adapter: EngineAdapter,
        jinja_macros: JinjaMacroRegistry,
        jinja_globals: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        from dbt.adapters.base.relation import Policy

        self.engine_adapter = engine_adapter
        self.jinja_macros = jinja_macros
        self.jinja_globals = jinja_globals or {}
        # All engines quote by default except Snowflake
        quote_param = engine_adapter.DIALECT != "snowflake"
        self.quote_policy = Policy(
            database=quote_param,
            schema=quote_param,
            identifier=quote_param,
        )

    def get_relation(self, database: str, schema: str, identifier: str) -> t.Optional[BaseRelation]:
        """
        Returns a single relation that matches the provided path
        """
        relations_list = self.list_relations(database, schema)
        matching_relations = [
            r
            for r in relations_list
            if r.identifier == identifier and r.schema == schema and r.database == database
        ]
        return seq_get(matching_relations, 0)

    def list_relations(self, database: t.Optional[str], schema: str) -> t.List[BaseRelation]:
        """
        Gets all relations in a given schema and optionally database

        TODO: Add caching functionality to avoid repeat visits to DB
        """
        from dbt.adapters.base import BaseRelation

        reference_relation = BaseRelation.create(
            database=database,
            schema=schema,
        )
        return self.list_relations_without_caching(reference_relation)

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> t.List[BaseRelation]:
        """
        Using the engine adapter, gets all the relations that match the given schema grain relation.
        """
        from dbt.adapters.base import BaseRelation
        from dbt.contracts.relation import RelationType

        assert schema_relation.schema is not None
        data_objects = self.engine_adapter._get_data_objects(
            schema_name=schema_relation.schema, catalog_name=schema_relation.database
        )
        relations = [
            BaseRelation.create(
                database=do.catalog,
                schema=do.schema_name,
                identifier=do.name,
                quote_policy=self.quote_policy,
                type=RelationType.External if do.type.is_unknown else RelationType(do.type.lower()),
            )
            for do in data_objects
        ]
        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> t.List[Column]:
        """
        Returns the columns for a given table grained relation
        """
        from dbt.adapters.base.column import Column

        return [
            Column.from_description(name=name, raw_data_type=dtype)
            for name, dtype in self.engine_adapter.columns(table_name=relation.render()).items()
        ]

    def dispatch(self, name: str, package: t.Optional[str] = None) -> t.Callable:
        """
        Returns a dialect-specific version of a macro with the given name.
        """
        dialect_name = f"{self.engine_adapter.dialect}__{name}"
        default_name = f"default__{name}"

        references_to_try = [
            MacroReference(package=package, name=dialect_name),
            MacroReference(package=package, name=default_name),
        ]

        for reference in references_to_try:
            macro_callable = self.jinja_macros.build_macro(
                reference, **{**self.jinja_globals, "adapter": self}
            )
            if macro_callable is not None:
                return macro_callable

        raise ConfigError(f"Macro '{name}', package '{package}' was not found.")

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> t.Tuple[AdapterResponse, agate.Table]:
        """
        Executes the given SQL statement and returns the results as a pandas dataframe
        """
        from dbt.adapters.base.impl import AdapterResponse
        from dbt.clients.agate_helper import empty_table

        from sqlmesh.dbt.util import pandas_to_agate

        # mypy bug: https://github.com/python/mypy/issues/10740
        exec_func: t.Callable[[str], None | pd.DataFrame] = (
            self.engine_adapter.fetchdf if fetch else self.engine_adapter.execute  # type: ignore
        )

        if auto_begin:
            # TODO: This could be a bug. I think dbt leaves the transaction open while we close immediately.
            with self.engine_adapter.transaction(TransactionType.DML):
                resp = exec_func(sql)
        else:
            resp = exec_func(sql)

        # TODO: Properly fill in adapter response
        if fetch:
            assert isinstance(resp, pd.DataFrame)
            return AdapterResponse("Success"), pandas_to_agate(resp)
        return AdapterResponse("Success"), empty_table()

    def quote(self, identifier: str) -> str:
        return exp.to_column(identifier).sql(dialect=self.engine_adapter.dialect, identify=True)
