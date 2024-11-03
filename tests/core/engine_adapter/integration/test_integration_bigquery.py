import typing as t
import pytest
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.helper import seq_get
from sqlmesh.core.engine_adapter import BigQueryEngineAdapter
from sqlmesh.core.engine_adapter.bigquery import _CLUSTERING_META_KEY
from sqlmesh.core.engine_adapter.shared import DataObject
import sqlmesh.core.dialect as d
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.plan import Plan
from tests.core.engine_adapter.integration import TestContext

pytestmark = [pytest.mark.engine, pytest.mark.remote, pytest.mark.bigquery]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "bigquery", "inttest_bigquery"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_get_alter_expressions_includes_clustering(
    ctx: TestContext, engine_adapter: BigQueryEngineAdapter
):
    clustered_table = ctx.table("clustered_table")
    clustered_differently_table = ctx.table("clustered_differently_table")
    normal_table = ctx.table("normal_table")

    engine_adapter.execute(
        f"CREATE TABLE {clustered_table.sql(dialect=ctx.dialect)} (c1 int, c2 timestamp) CLUSTER BY c1"
    )
    engine_adapter.execute(
        f"CREATE TABLE {clustered_differently_table.sql(dialect=ctx.dialect)} (c1 int, c2 timestamp) CLUSTER BY c1, c2"
    )
    engine_adapter.execute(
        f"CREATE TABLE {normal_table.sql(dialect=ctx.dialect)} (c1 int, c2 timestamp)"
    )

    metadata = engine_adapter.get_data_objects(
        normal_table.db, {clustered_table.name, clustered_differently_table.name, normal_table.name}
    )
    clustered_table_metadata = next(md for md in metadata if md.name == clustered_table.name)
    clustered_differently_table_metadata = next(
        md for md in metadata if md.name == clustered_differently_table.name
    )
    normal_table_metadata = next(md for md in metadata if md.name == normal_table.name)

    assert clustered_table_metadata.clustering_key == "(c1)"
    assert clustered_differently_table_metadata.clustering_key == "(c1,c2)"
    assert normal_table_metadata.clustering_key is None

    assert len(engine_adapter.get_alter_expressions(normal_table, normal_table)) == 0
    assert len(engine_adapter.get_alter_expressions(clustered_table, clustered_table)) == 0

    # alter table drop clustered
    clustered_to_normal = engine_adapter.get_alter_expressions(clustered_table, normal_table)
    assert len(clustered_to_normal) == 1
    assert clustered_to_normal[0].meta[_CLUSTERING_META_KEY] == (clustered_table, None)

    # alter table add clustered
    normal_to_clustered = engine_adapter.get_alter_expressions(normal_table, clustered_table)
    assert len(normal_to_clustered) == 1
    assert normal_to_clustered[0].meta[_CLUSTERING_META_KEY] == (
        normal_table,
        [exp.to_column("c1")],
    )

    # alter table change clustering (c1 -> (c1, c2))
    clustered_to_clustered_differently = engine_adapter.get_alter_expressions(
        clustered_table, clustered_differently_table
    )
    assert len(clustered_to_clustered_differently) == 1
    assert clustered_to_clustered_differently[0].meta[_CLUSTERING_META_KEY] == (
        clustered_table,
        [exp.to_column("c1"), exp.to_column("c2")],
    )

    # alter table change clustering ((c1, c2) -> c1)
    clustered_differently_to_clustered = engine_adapter.get_alter_expressions(
        clustered_differently_table, clustered_table
    )
    assert len(clustered_differently_to_clustered) == 1
    assert clustered_differently_to_clustered[0].meta[_CLUSTERING_META_KEY] == (
        clustered_differently_table,
        [exp.to_column("c1")],
    )


def test_mutating_clustered_by_forward_only(
    ctx: TestContext, engine_adapter: BigQueryEngineAdapter
):
    model_name = ctx.table("TEST")

    sqlmesh = ctx.create_context()

    def _create_model(**kwargs: t.Any) -> SqlModel:
        extra_props = "\n".join([f"{k} {v}," for k, v in kwargs.items()])
        return t.cast(
            SqlModel,
            load_sql_based_model(
                d.parse(
                    f"""
                MODEL (
                    name {model_name},
                    kind INCREMENTAL_BY_TIME_RANGE (
                        time_column partitiondate
                    ),
                    {extra_props}
                    start '2021-01-01',
                    cron '@daily',
                    dialect 'bigquery'
                );

                select 1 as ID, current_date() as partitiondate
                """
                )
            ),
        )

    def _get_data_object(table: exp.Table) -> DataObject:
        data_object = seq_get(engine_adapter.get_data_objects(table.db, {table.name}), 0)
        if not data_object:
            raise ValueError(f"Expected metadata for {table}")
        return data_object

    m1 = _create_model()
    m2 = _create_model(clustered_by="partitiondate")
    m3 = _create_model(clustered_by="(id, partitiondate)")

    # Initial plan - non-clustered table
    sqlmesh.upsert_model(m1)
    plan_1: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True)
    assert len(plan_1.snapshots) == 1
    target_table_1 = exp.to_table(list(plan_1.snapshots.values())[0].table_name())
    quote_identifiers(target_table_1)

    assert not _get_data_object(target_table_1).is_clustered

    # Next plan - add clustering key (non-clustered -> clustered)
    sqlmesh.upsert_model(m2)
    plan_2: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True, forward_only=True)
    assert len(plan_2.snapshots) == 1
    target_table_2 = exp.to_table(list(plan_2.snapshots.values())[0].table_name())
    quote_identifiers(target_table_2)

    assert target_table_1 == target_table_2

    metadata = _get_data_object(target_table_1)
    assert metadata.is_clustered
    assert metadata.clustering_key == "(partitiondate)"

    # Next plan - change clustering key (clustered -> clustered differently)
    sqlmesh.upsert_model(m3)
    plan_3: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True, forward_only=True)
    assert len(plan_3.snapshots) == 1
    target_table_3 = exp.to_table(list(plan_3.snapshots.values())[0].table_name())
    quote_identifiers(target_table_3)

    assert target_table_1 == target_table_3

    metadata = _get_data_object(target_table_1)
    assert metadata.is_clustered
    assert metadata.clustering_key == "(id,partitiondate)"

    # Next plan - drop clustering key
    sqlmesh.upsert_model(m1)
    plan_4: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True, forward_only=True)
    assert len(plan_4.snapshots) == 1
    target_table_4 = exp.to_table(list(plan_4.snapshots.values())[0].table_name())
    quote_identifiers(target_table_4)

    assert target_table_1 == target_table_4

    metadata = _get_data_object(target_table_1)
    assert not metadata.is_clustered


def test_fetch_schema_of_information_schema_tables(
    ctx: TestContext, engine_adapter: BigQueryEngineAdapter
):
    # We produce Table(this=Dot(this=INFORMATION_SCHEMA, expression=TABLES)) here,
    # otherwise `db` or `catalog` would be set, which is not the right representation
    information_schema_tables = exp.to_table("_._.INFORMATION_SCHEMA.TABLES")
    information_schema_tables.set("db", None)
    information_schema_tables.set("catalog", None)

    source = ctx.table(information_schema_tables)

    expected_columns_to_types = {
        "table_catalog": exp.DataType.build("TEXT"),
        "table_schema": exp.DataType.build("TEXT"),
        "table_name": exp.DataType.build("TEXT"),
        "table_type": exp.DataType.build("TEXT"),
        "is_insertable_into": exp.DataType.build("TEXT"),
        "is_typed": exp.DataType.build("TEXT"),
        "creation_time": exp.DataType.build("TIMESTAMPTZ"),
        "base_table_catalog": exp.DataType.build("TEXT"),
        "base_table_schema": exp.DataType.build("TEXT"),
        "base_table_name": exp.DataType.build("TEXT"),
        "snapshot_time_ms": exp.DataType.build("TIMESTAMPTZ"),
        "ddl": exp.DataType.build("TEXT"),
        "default_collation_name": exp.DataType.build("TEXT"),
        "upsert_stream_apply_watermark": exp.DataType.build("TIMESTAMPTZ"),
        "replica_source_catalog": exp.DataType.build("TEXT"),
        "replica_source_schema": exp.DataType.build("TEXT"),
        "replica_source_name": exp.DataType.build("TEXT"),
        "replication_status": exp.DataType.build("TEXT"),
        "replication_error": exp.DataType.build("TEXT"),
        "is_change_history_enabled": exp.DataType.build("TEXT"),
        "sync_status": exp.DataType.build(
            "STRUCT<last_completion_time TIMESTAMPTZ, error_time TIMESTAMPTZ, error STRUCT<reason TEXT, location TEXT, message TEXT>>"
        ),
    }

    assert expected_columns_to_types == engine_adapter.columns(source.sql())
