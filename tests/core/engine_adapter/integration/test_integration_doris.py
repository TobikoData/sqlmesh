from __future__ import annotations

import typing as t
import pytest
import pandas as pd
from sqlglot import exp, parse_one
from pytest import FixtureRequest

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
)

pytestmark = [pytest.mark.doris, pytest.mark.engine]


@pytest.fixture(params=list(generate_pytest_params([ENGINES_BY_NAME["doris"]])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str, str], t.Iterable[TestContext]],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> DorisEngineAdapter:
    assert isinstance(ctx.engine_adapter, DorisEngineAdapter)
    return ctx.engine_adapter


def test_doris_table_models(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris-specific table models: UNIQUE, DUPLICATE, AGGREGATE"""

    # Test UNIQUE KEY table (default)
    unique_table = ctx.table("unique_test")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("INT"),
    }

    # Create UNIQUE KEY table
    engine_adapter.create_table(
        unique_table,
        columns_to_types=columns_to_types,
        table_properties={
            "TABLE_MODEL": "UNIQUE",
            "UNIQUE_KEY": ["id", "name"],
            "DISTRIBUTED_BY": "HASH(id)",
            "BUCKETS": "10",
        },
        table_description="Test UNIQUE KEY table",
    )

    # Verify table was created
    objects = engine_adapter.get_data_objects(unique_table.db)
    table_names = [obj.name for obj in objects if obj.type.is_table]
    assert unique_table.name in table_names

    # Test DUPLICATE table
    duplicate_table = ctx.table("duplicate_test")
    engine_adapter.create_table(
        duplicate_table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "DUPLICATE", "DISTRIBUTED_BY": "HASH(id)", "BUCKETS": "5"},
    )

    # Test AGGREGATE table
    aggregate_table = ctx.table("aggregate_test")
    agg_columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "sum_value": exp.DataType.build("INT"),
    }

    engine_adapter.create_table(
        aggregate_table,
        columns_to_types=agg_columns_to_types,
        table_properties={
            "TABLE_MODEL": "AGGREGATE",
            "AGGREGATE_KEY": ["id", "name"],
            "DISTRIBUTED_BY": "HASH(id)",
            "BUCKETS": "8",
        },
    )

    # Verify all tables exist
    objects = engine_adapter.get_data_objects(unique_table.db)
    table_names = [obj.name for obj in objects if obj.type.is_table]
    assert unique_table.name in table_names
    assert duplicate_table.name in table_names
    assert aggregate_table.name in table_names


def test_doris_partitioning(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris partitioning features"""

    # Test RANGE partitioning
    range_table = ctx.table("range_partitioned")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "date_col": exp.DataType.build("DATE"),
        "value": exp.DataType.build("STRING"),
    }

    engine_adapter.create_table(
        range_table,
        columns_to_types=columns_to_types,
        partitioned_by=[parse_one("date_col")],
        table_properties={
            "TABLE_MODEL": "UNIQUE",
            "UNIQUE_KEY": ["id"],
            "DISTRIBUTED_BY": "HASH(id)",
            "BUCKETS": "10",
            "PARTITION_TYPE": "RANGE",
        },
    )

    # Test LIST partitioning
    list_table = ctx.table("list_partitioned")
    engine_adapter.create_table(
        list_table,
        columns_to_types=columns_to_types,
        partitioned_by=[parse_one("date_col")],
        table_properties={
            "TABLE_MODEL": "DUPLICATE",
            "DISTRIBUTED_BY": "HASH(id)",
            "BUCKETS": "5",
            "PARTITION_TYPE": "LIST",
        },
    )

    # Verify tables exist
    objects = engine_adapter.get_data_objects(range_table.db)
    table_names = [obj.name for obj in objects if obj.type.is_table]
    assert range_table.name in table_names
    assert list_table.name in table_names


def test_doris_indexes(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris index creation"""

    table = ctx.table("index_test")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "content": exp.DataType.build("TEXT"),
        "tags": exp.DataType.build("STRING"),
    }

    # Create table first
    engine_adapter.create_table(
        table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "UNIQUE", "UNIQUE_KEY": ["id"], "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Test INVERTED index
    engine_adapter.create_index(
        table,
        "idx_content_inverted",
        ("content",),
        index_type="INVERTED",
        comment="Inverted index for full text search",
    )

    # Test BLOOMFILTER index
    engine_adapter.create_index(
        table, "idx_name_bloom", ("name",), index_type="BLOOMFILTER", comment="Bloom filter for name column"
    )

    # Test NGRAM_BF index
    engine_adapter.create_index(
        table,
        "idx_tags_ngram",
        ("tags",),
        index_type="NGRAM_BF",
        properties={"gram_size": "3", "bf_size": "256"},
        comment="N-gram bloom filter for tags",
    )


def test_doris_views_and_materialized_views(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris view and materialized view creation"""

    # Create base table
    base_table = ctx.table("base_for_views")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("INT"),
    }

    engine_adapter.create_table(
        base_table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "UNIQUE", "UNIQUE_KEY": ["id"], "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Insert test data
    test_data = pd.DataFrame(
        [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ]
    )

    ctx.engine_adapter.replace_query(
        base_table, ctx.input_data(test_data, columns_to_types), columns_to_types=columns_to_types
    )

    # Test regular view
    view_name = ctx.table("test_view")
    query = exp.select("id", "name", "value * 2 AS double_value").from_(base_table)

    engine_adapter.create_view(view_name, query, replace=True, materialized=False)

    # Test materialized view
    mv_name = ctx.table("test_mv")
    mv_query = exp.select("name", "SUM(value) AS total_value").from_(base_table).group_by("name")

    engine_adapter.create_view(
        mv_name,
        mv_query,
        replace=True,
        materialized=True,
        table_description="Test materialized view",
        materialized_properties={"build": "IMMEDIATE", "refresh": "MANUAL"},
    )

    # Verify views exist
    objects = engine_adapter.get_data_objects(base_table.db)
    view_names = [obj.name for obj in objects if obj.type.is_view]
    mv_names = [obj.name for obj in objects if obj.type.is_materialized_view]

    assert view_name.name in view_names
    assert mv_name.name in mv_names

    # Drop views
    engine_adapter.drop_view(view_name, materialized=False)
    engine_adapter.drop_view(mv_name, materialized=True)


def test_doris_table_comments(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris table and column comments"""

    table = ctx.table("comment_test")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "description": exp.DataType.build("TEXT"),
    }

    column_descriptions = {
        "id": "Primary key identifier",
        "name": "User name field",
        "description": "Long text description field",
    }

    engine_adapter.create_table(
        table,
        columns_to_types=columns_to_types,
        table_description="Test table with comments",
        column_descriptions=column_descriptions,
        table_properties={"TABLE_MODEL": "UNIQUE", "UNIQUE_KEY": ["id"], "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Verify table comment
    table_comment = ctx.get_table_comment(table.db, table.name)
    assert table_comment == "Test table with comments"

    # Verify column comments
    col_comments = ctx.get_column_comments(table.db, table.name)
    assert col_comments.get("id") == "Primary key identifier"
    assert col_comments.get("name") == "User name field"
    assert col_comments.get("description") == "Long text description field"


def test_doris_create_table_like(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris CREATE TABLE LIKE functionality"""

    # Create source table
    source_table = ctx.table("source_table")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("DECIMAL(10,2)"),
    }

    engine_adapter.create_table(
        source_table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "UNIQUE", "UNIQUE_KEY": ["id"], "DISTRIBUTED_BY": "HASH(id)", "BUCKETS": "10"},
    )

    # Create table like source
    target_table = ctx.table("target_table")
    engine_adapter.create_table_like(target_table, source_table, exists=True)

    # Verify both tables exist
    objects = engine_adapter.get_data_objects(source_table.db)
    table_names = [obj.name for obj in objects if obj.type.is_table]
    assert source_table.name in table_names
    assert target_table.name in table_names


def test_doris_data_operations(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test basic data operations with Doris"""

    table = ctx.table("data_ops_test")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("INT"),
        "created_at": exp.DataType.build("DATETIME"),
    }

    # Create table
    engine_adapter.create_table(
        table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "UNIQUE", "UNIQUE_KEY": ["id"], "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Insert initial data
    initial_data = pd.DataFrame(
        [
            {"id": 1, "name": "Alice", "value": 100, "created_at": "2024-01-01 10:00:00"},
            {"id": 2, "name": "Bob", "value": 200, "created_at": "2024-01-02 11:00:00"},
        ]
    )

    ctx.engine_adapter.replace_query(
        table, ctx.input_data(initial_data, columns_to_types), columns_to_types=columns_to_types
    )

    # Verify data
    current_data = ctx.get_current_data(table)
    assert len(current_data) == 2
    assert current_data["id"].tolist() == [1, 2]

    # Test update (should work with UNIQUE KEY model)
    update_data = pd.DataFrame(
        [
            {"id": 1, "name": "Alice Updated", "value": 150, "created_at": "2024-01-01 10:00:00"},
            {"id": 3, "name": "Charlie", "value": 300, "created_at": "2024-01-03 12:00:00"},
        ]
    )

    ctx.engine_adapter.replace_query(
        table, ctx.input_data(update_data, columns_to_types), columns_to_types=columns_to_types
    )

    # Verify updated data
    updated_data = ctx.get_current_data(table)
    assert len(updated_data) == 3
    alice_row = updated_data[updated_data["id"] == 1].iloc[0]
    assert alice_row["name"] == "Alice Updated"
    assert alice_row["value"] == 150


def test_doris_connection_and_catalog(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test Doris connection and catalog operations"""

    # Test ping
    engine_adapter.ping()

    # Test current catalog
    current_catalog = engine_adapter.get_current_catalog()
    assert current_catalog == "internal"

    # Test schema operations
    test_schema = ctx.schema("doris_test_schema")
    engine_adapter.create_schema(test_schema)

    # Verify schema exists by creating a table in it
    table = ctx.table("test_table", schema="doris_test_schema")
    columns_to_types = {"id": exp.DataType.build("INT")}

    engine_adapter.create_table(
        table,
        columns_to_types=columns_to_types,
        table_properties={"TABLE_MODEL": "DUPLICATE", "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Verify table exists in schema
    objects = engine_adapter.get_data_objects(test_schema)
    table_names = [obj.name for obj in objects if obj.type.is_table]
    assert table.name in table_names


def test_doris_comment_truncation(ctx: TestContext, engine_adapter: DorisEngineAdapter):
    """Test comment truncation for long comments"""

    table = ctx.table("long_comment_test")
    columns_to_types = {
        "id": exp.DataType.build("INT"),
        "data": exp.DataType.build("STRING"),
    }

    # Create very long comments
    long_table_comment = "A" * 3000  # Exceeds MAX_TABLE_COMMENT_LENGTH (2048)
    long_column_comment = "B" * 300  # Exceeds MAX_COLUMN_COMMENT_LENGTH (255)

    column_descriptions = {
        "id": "Short comment",
        "data": long_column_comment,
    }

    engine_adapter.create_table(
        table,
        columns_to_types=columns_to_types,
        table_description=long_table_comment,
        column_descriptions=column_descriptions,
        table_properties={"TABLE_MODEL": "DUPLICATE", "DISTRIBUTED_BY": "HASH(id)"},
    )

    # Verify comments were truncated
    table_comment = ctx.get_table_comment(table.db, table.name)
    col_comments = ctx.get_column_comments(table.db, table.name)

    # Table comment should be truncated to 2048 chars with "..." at the end
    assert table_comment is not None
    assert len(table_comment) <= 2048
    assert table_comment.endswith("...")

    # Column comment should be truncated to 255 chars with "..." at the end
    data_comment = col_comments.get("data", "")
    assert len(data_comment) <= 255
    assert data_comment.endswith("...")

    # Short comment should remain unchanged
    assert col_comments.get("id") == "Short comment"
