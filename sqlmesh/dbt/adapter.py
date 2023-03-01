from __future__ import annotations

import typing as t

from sqlglot.helper import seq_get

from sqlmesh.core.engine_adapter import EngineAdapter

if t.TYPE_CHECKING:
    from dbt.adapters.base import BaseRelation
    from dbt.adapters.base.column import Column


class Adapter:
    def __init__(self, engine_adapter: EngineAdapter):
        from dbt.adapters.base.relation import Policy

        self.engine_adapter = engine_adapter
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
