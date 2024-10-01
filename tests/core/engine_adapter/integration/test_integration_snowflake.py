import typing as t
import pytest
from sqlglot import exp
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.helper import seq_get
from sqlmesh.core.engine_adapter import SnowflakeEngineAdapter
from sqlmesh.core.engine_adapter.snowflake import SnowflakeDataObject
import sqlmesh.core.dialect as d
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.plan import Plan
from tests.core.engine_adapter.integration import TestContext

pytestmark = [pytest.mark.engine, pytest.mark.remote, pytest.mark.snowflake]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "snowflake", "inttest_snowflake"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_get_alter_expressions_includes_clustering(
    ctx: TestContext, engine_adapter: SnowflakeEngineAdapter
):
    ctx.init()

    clustered_table = ctx.table("clustered_table")
    clustered_differently_table = ctx.table("clustered_differently_table")
    normal_table = ctx.table("normal_table")

    engine_adapter.execute(f"CREATE TABLE {clustered_table} (c1 int, c2 timestamp) CLUSTER BY (c1)")
    engine_adapter.execute(
        f"CREATE TABLE {clustered_differently_table} (c1 int, c2 timestamp) CLUSTER BY (c1, to_date(c2))"
    )
    engine_adapter.execute(f"CREATE TABLE {normal_table} (c1 int, c2 timestamp)")

    assert len(engine_adapter.get_alter_expressions(normal_table, normal_table)) == 0
    assert len(engine_adapter.get_alter_expressions(clustered_table, clustered_table)) == 0

    # alter table drop clustered
    clustered_to_normal = engine_adapter.get_alter_expressions(clustered_table, normal_table)
    assert len(clustered_to_normal) == 1
    assert (
        clustered_to_normal[0].sql(dialect=ctx.dialect)
        == f"ALTER TABLE {clustered_table} DROP CLUSTERING KEY"
    )

    # alter table add clustered
    normal_to_clustered = engine_adapter.get_alter_expressions(normal_table, clustered_table)
    assert len(normal_to_clustered) == 1
    assert (
        normal_to_clustered[0].sql(dialect=ctx.dialect)
        == f"ALTER TABLE {normal_table} CLUSTER BY (c1)"
    )

    # alter table change clustering
    clustered_to_clustered_differently = engine_adapter.get_alter_expressions(
        clustered_table, clustered_differently_table
    )
    assert len(clustered_to_clustered_differently) == 1
    assert (
        clustered_to_clustered_differently[0].sql(dialect=ctx.dialect)
        == f"ALTER TABLE {clustered_table} CLUSTER BY (c1, TO_DATE(c2))"
    )

    # alter table change clustering
    clustered_differently_to_clustered = engine_adapter.get_alter_expressions(
        clustered_differently_table, clustered_table
    )
    assert len(clustered_differently_to_clustered) == 1
    assert (
        clustered_differently_to_clustered[0].sql(dialect=ctx.dialect)
        == f"ALTER TABLE {clustered_differently_table} CLUSTER BY (c1)"
    )


def test_adding_clustered_by_forward_only(ctx: TestContext, engine_adapter: SnowflakeEngineAdapter):
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
                        time_column PARTITIONDATE
                    ),
                    {extra_props}
                    start '2021-01-01',
                    cron '@daily',
                    dialect 'snowflake'
                );

                select 1 as ID, current_timestamp() as PARTITIONDATE
                """
                )
            ),
        )

    def _get_data_object(table: exp.Table) -> SnowflakeDataObject:
        data_object = seq_get(engine_adapter.get_data_objects(table.db, {table.name}), 0)
        if not data_object:
            raise ValueError(f"Expected metadata for {table}")
        return t.cast(SnowflakeDataObject, data_object)

    m1 = _create_model()
    m2 = _create_model(clustered_by="PARTITIONDATE")
    m3 = _create_model(clustered_by="(ID, PARTITIONDATE)")

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
    assert metadata.clustering_key == 'LINEAR("PARTITIONDATE")'

    # Next plan - change clustering key (clustered -> clustered differently)
    sqlmesh.upsert_model(m3)
    plan_3: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True, forward_only=True)
    assert len(plan_3.snapshots) == 1
    target_table_3 = exp.to_table(list(plan_3.snapshots.values())[0].table_name())
    quote_identifiers(target_table_3)

    assert target_table_1 == target_table_3

    metadata = _get_data_object(target_table_1)
    assert metadata.is_clustered
    assert metadata.clustering_key == 'LINEAR("ID", "PARTITIONDATE")'

    # Next plan - drop clustering key
    sqlmesh.upsert_model(m1)
    plan_4: Plan = sqlmesh.plan(auto_apply=True, no_prompts=True, forward_only=True)
    assert len(plan_4.snapshots) == 1
    target_table_4 = exp.to_table(list(plan_4.snapshots.values())[0].table_name())
    quote_identifiers(target_table_4)

    assert target_table_1 == target_table_4

    metadata = _get_data_object(target_table_1)
    assert not metadata.is_clustered
