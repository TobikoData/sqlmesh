import typing as t
import pytest
import pandas as pd
import datetime
from sqlmesh.core.engine_adapter import AthenaEngineAdapter
from sqlmesh.utils.aws import parse_s3_uri
from sqlmesh.utils.pandas import columns_to_types_from_df
from tests.core.engine_adapter.integration import TestContext
from sqlglot import exp

pytestmark = [pytest.mark.remote, pytest.mark.engine, pytest.mark.athena]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "athena", "inttest_athena"


@pytest.fixture
def test_type() -> str:
    return "query"


@pytest.fixture
def s3(engine_adapter: AthenaEngineAdapter) -> t.Any:
    return engine_adapter._s3_client


def s3_list_objects(s3: t.Any, location: str, **list_objects_kwargs: t.Any) -> t.List[str]:
    bucket, prefix = parse_s3_uri(location)
    lst = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        lst.extend([o["Key"] for o in page.get("Contents", [])])
    return lst


def test_clear_partition_data(ctx: TestContext, engine_adapter: AthenaEngineAdapter, s3: t.Any):
    base_uri = engine_adapter.s3_warehouse_location_or_raise
    assert len(s3_list_objects(s3, base_uri)) == 0

    src_table = ctx.table("src_table")
    test_table = ctx.table("test_table")

    base_data = pd.DataFrame(
        [
            {"id": 1, "ts": datetime.datetime(2023, 1, 1, 12, 13, 14)},
            {"id": 2, "ts": datetime.datetime(2023, 1, 2, 8, 10, 0)},
            {"id": 3, "ts": datetime.datetime(2023, 1, 3, 16, 5, 14)},
        ]
    )

    engine_adapter.ctas(
        table_name=src_table,
        query_or_df=base_data,
    )

    sqlmesh_context, model = ctx.upsert_sql_model(
        f"""
        MODEL (
            name {test_table},
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds
            ),
            start '2023-01-01'
        );

        SELECT
            id, ts, (ts::date)::varchar as ds
        FROM {src_table}
        WHERE ts BETWEEN @start_dt AND @end_dt
        """
    )

    plan = sqlmesh_context.plan(no_prompts=True, auto_apply=True)
    assert len(plan.snapshots) == 1
    test_table_snapshot = list(plan.snapshots.values())[0]

    files_before = s3_list_objects(s3, base_uri)
    assert len(files_before) > 0

    # src_table should have no partitions
    with pytest.raises(Exception, match=r".*TABLE_NOT_FOUND.*\$partitions"):
        engine_adapter._list_partitions(src_table)

    # test_table physical snapshot table should have 3 partitions
    test_table_physical_name = exp.to_table(test_table_snapshot.table_name())
    partitions = engine_adapter._list_partitions(test_table_physical_name, where=None)
    assert len(partitions) == 3
    assert [p[0] for p in partitions] == [["2023-01-01"], ["2023-01-02"], ["2023-01-03"]]

    assert engine_adapter.fetchone(f"select count(*) from {test_table}")[0] == 3  # type: ignore

    # clear a partition
    assert model.time_column
    engine_adapter._clear_partition_data(
        table=test_table_physical_name,
        where=exp.Between(
            this=model.time_column.column,
            low=exp.Literal.string("2023-01-01"),
            high=exp.Literal.string("2023-01-01"),
        ),
    )
    partitions = engine_adapter._list_partitions(test_table_physical_name, where=None)
    assert len(partitions) == 2
    assert [p[0] for p in partitions] == [["2023-01-02"], ["2023-01-03"]]

    # test that only S3 data for that partition was affected
    files_after = s3_list_objects(s3, base_uri)
    assert len(files_after) == len(files_before) - 1
    assert len([f for f in files_before if "ds=2023-01-01" in f]) == 1
    assert len([f for f in files_after if "ds=2023-01-01" in f]) == 0

    assert engine_adapter.fetchone(f"select count(*) from {test_table}")[0] == 2  # type: ignore


def test_clear_partition_data_multiple_columns(
    ctx: TestContext, engine_adapter: AthenaEngineAdapter, s3: t.Any
):
    base_uri = engine_adapter.s3_warehouse_location_or_raise

    src_table = ctx.table("src_table")
    test_table = ctx.table("test_table")

    base_data = pd.DataFrame(
        [
            {"id": 1, "ts": datetime.datetime(2023, 1, 1, 12, 13, 14), "system": "dev"},
            {"id": 2, "ts": datetime.datetime(2023, 1, 1, 8, 13, 14), "system": "prod"},
            {"id": 3, "ts": datetime.datetime(2023, 1, 2, 11, 10, 0), "system": "dev"},
            {"id": 4, "ts": datetime.datetime(2023, 1, 2, 8, 10, 0), "system": "dev"},
            {"id": 5, "ts": datetime.datetime(2023, 1, 3, 16, 5, 14), "system": "dev"},
            {"id": 6, "ts": datetime.datetime(2023, 1, 3, 16, 5, 14), "system": "prod"},
        ]
    )

    engine_adapter.ctas(
        table_name=src_table,
        query_or_df=base_data,
    )

    sqlmesh_context, model = ctx.upsert_sql_model(
        f"""
        MODEL (
            name {test_table},
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds
            ),
            partitioned_by (ds, system),
            start '2023-01-01'
        );

        SELECT
            id, ts, (ts::date)::varchar as ds, system
        FROM {src_table}
        WHERE ts BETWEEN @start_dt AND @end_dt
        """
    )

    plan = sqlmesh_context.plan(no_prompts=True, auto_apply=True)
    assert len(plan.snapshots) == 1
    test_table_snapshot = list(plan.snapshots.values())[0]
    test_table_physical_name = exp.to_table(test_table_snapshot.table_name())

    partitions = engine_adapter._list_partitions(test_table_physical_name, where=None)
    assert len(partitions) == 5
    assert [p[0] for p in partitions] == [
        ["2023-01-01", "dev"],
        ["2023-01-01", "prod"],
        ["2023-01-02", "dev"],
        ["2023-01-03", "dev"],
        ["2023-01-03", "prod"],
    ]

    files_before = s3_list_objects(s3, base_uri)
    assert len(files_before) > 0

    assert engine_adapter.fetchone(f"select count(*) from {test_table}")[0] == 6  # type: ignore

    # this should clear 2 partitions, ["2023-01-01", "dev"] and ["2023-01-01", "prod"]
    assert model.time_column
    engine_adapter._clear_partition_data(
        table=test_table_physical_name,
        where=exp.Between(
            this=model.time_column.column,
            low=exp.Literal.string("2023-01-01"),
            high=exp.Literal.string("2023-01-01"),
        ),
    )

    partitions = engine_adapter._list_partitions(test_table_physical_name, where=None)
    assert len(partitions) == 3
    assert [p[0] for p in partitions] == [
        ["2023-01-02", "dev"],
        ["2023-01-03", "dev"],
        ["2023-01-03", "prod"],
    ]

    files_after = s3_list_objects(s3, base_uri)
    assert len(files_after) == len(files_before) - 2

    def _match_partition(location_list: t.List[str], match: str):
        return any(match in location for location in location_list)

    assert _match_partition(files_before, "ds=2023-01-01/system=dev")
    assert _match_partition(files_before, "ds=2023-01-01/system=prod")
    assert not _match_partition(files_after, "ds=2023-01-01/system=dev")
    assert not _match_partition(files_after, "ds=2023-01-01/system=prod")

    assert engine_adapter.fetchone(f"select count(*) from {test_table}")[0] == 4  # type: ignore


def test_hive_truncate_table(ctx: TestContext, engine_adapter: AthenaEngineAdapter, s3: t.Any):
    base_uri = engine_adapter.s3_warehouse_location_or_raise

    table_1 = ctx.table("table_one")
    table_2 = ctx.table("table_two")

    base_data = pd.DataFrame(
        [
            {"id": 1, "ts": datetime.datetime(2023, 1, 1, 12, 13, 14), "system": "dev"},
            {"id": 2, "ts": datetime.datetime(2023, 1, 1, 8, 13, 14), "system": "prod"},
            {"id": 3, "ts": datetime.datetime(2023, 1, 2, 11, 10, 0), "system": "dev"},
            {"id": 4, "ts": datetime.datetime(2023, 1, 2, 8, 10, 0), "system": "dev"},
            {"id": 5, "ts": datetime.datetime(2023, 1, 3, 16, 5, 14), "system": "dev"},
            {"id": 6, "ts": datetime.datetime(2023, 1, 3, 16, 5, 14), "system": "prod"},
        ]
    )

    assert len(s3_list_objects(s3, base_uri)) == 0

    engine_adapter.ctas(table_name=table_1, query_or_df=base_data)

    engine_adapter.ctas(table_name=table_2, query_or_df=base_data)

    all_files = s3_list_objects(s3, base_uri)
    assert len(all_files) > 0

    table_1_location = engine_adapter._query_table_s3_location(table_1)
    table_2_location = engine_adapter._query_table_s3_location(table_2)

    table_1_files = s3_list_objects(s3, table_1_location)
    table_2_files = s3_list_objects(s3, table_2_location)

    assert len(table_1_files) < len(all_files)
    assert len(table_2_files) < len(all_files)
    assert len(table_1_files) + len(table_2_files) == len(all_files)

    assert engine_adapter.fetchone(f"select count(*) from {table_1}")[0] == 6  # type: ignore
    engine_adapter._truncate_table(table_1)
    assert len(s3_list_objects(s3, table_1_location)) == 0
    assert len(s3_list_objects(s3, table_2_location)) == len(table_2_files)

    assert engine_adapter.fetchone(f"select count(*) from {table_1}")[0] == 0  # type: ignore

    # check truncating an empty table doesnt throw an error
    engine_adapter._truncate_table(table_1)


def test_hive_drop_table_removes_data(ctx: TestContext, engine_adapter: AthenaEngineAdapter):
    # check no exception with dropping a table that doesnt exist
    engine_adapter.drop_table("nonexist")

    seed_table = ctx.table("seed")

    data = pd.DataFrame(
        [
            {"id": 1, "name": "one"},
        ]
    )

    columns_to_types = columns_to_types_from_df(data)

    engine_adapter.create_table(
        table_name=seed_table, columns_to_types=columns_to_types, exists=False
    )
    engine_adapter.insert_append(
        table_name=seed_table, query_or_df=data, columns_to_types=columns_to_types
    )
    assert engine_adapter.fetchone(f"select count(*) from {seed_table}")[0] == 1  # type: ignore

    # By default, dropping a Hive table leaves its data in S3 so creating a new table with the same name / location picks up the old data
    # This ensures that our drop table logic to delete the data from S3 is working
    engine_adapter.drop_table(seed_table, exists=False)
    engine_adapter.create_table(
        table_name=seed_table, columns_to_types=columns_to_types, exists=False
    )
    assert engine_adapter.fetchone(f"select count(*) from {seed_table}")[0] == 0  # type: ignore


def test_hive_replace_query_same_schema(ctx: TestContext, engine_adapter: AthenaEngineAdapter):
    seed_table = ctx.table("seed")

    data = pd.DataFrame(
        [
            {"id": 1, "name": "one"},
            {"id": 2, "name": "two"},
        ]
    )

    assert not engine_adapter.table_exists(seed_table)

    engine_adapter.replace_query(table_name=seed_table, query_or_df=data)

    assert engine_adapter.fetchone(f"select count(*) from {seed_table}")[0] == 2  # type: ignore

    data.loc[len(data)] = [3, "three"]  # type: ignore

    engine_adapter.replace_query(table_name=seed_table, query_or_df=data)

    assert engine_adapter.fetchone(f"select count(*) from {seed_table}")[0] == 3  # type: ignore


def test_hive_replace_query_new_schema(ctx: TestContext, engine_adapter: AthenaEngineAdapter):
    seed_table = ctx.table("seed")

    orig_data = pd.DataFrame(
        [
            {"id": 1, "name": "one"},
            {"id": 2, "name": "two"},
        ]
    )

    new_data = pd.DataFrame(
        [
            {"foo": 1, "bar": "one", "ts": datetime.datetime(2023, 1, 1)},
        ]
    )

    engine_adapter.replace_query(table_name=seed_table, query_or_df=orig_data)

    assert engine_adapter.fetchall(f"select id, name from {seed_table} order by id") == [
        (1, "one"),
        (2, "two"),
    ]

    engine_adapter.replace_query(table_name=seed_table, query_or_df=new_data)

    with pytest.raises(Exception, match=r".*COLUMN_NOT_FOUND.*"):
        assert engine_adapter.fetchall(f"select id, name from {seed_table}")

    assert engine_adapter.fetchone(f"select foo, bar, ts from {seed_table}") == (
        1,
        "one",
        datetime.datetime(2023, 1, 1),
    )
