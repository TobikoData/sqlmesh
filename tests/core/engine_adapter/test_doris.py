import typing as t

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from tests.core.engine_adapter import to_sql_calls

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from sqlmesh.utils.errors import UnsupportedCatalogOperationError

from pytest_mock.plugin import MockerFixture

pytestmark = pytest.mark.doris


def test_create_view(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)
    # check view properties
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        view_properties={"a": exp.convert(1)},
    )

    assert to_sql_calls(adapter) == [
        "DROP VIEW IF EXISTS `test_view`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
    ]


def test_create_view_with_comment(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        columns_to_types={"a": exp.DataType.build("INT")},
        table_description="test_description",
        column_descriptions={"a": "test_column_description"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE VIEW `test_view` (`a` COMMENT 'test_column_description') COMMENT 'test_description' AS SELECT `a` FROM `tbl`",
    ]


def test_create_materialized_view(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT")},
    )
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT")},
    )

    assert to_sql_calls(adapter) == [
        "DROP MATERIALIZED VIEW IF EXISTS `test_view`",
        "CREATE MATERIALIZED VIEW `test_view` (`a`) AS SELECT `a` FROM `tbl`",
        "CREATE MATERIALIZED VIEW `test_view` (`a`) AS SELECT `a` FROM `tbl`",
    ]

    adapter.cursor.reset_mock()
    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM tbl"),
        replace=False,
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description", "b": "test_column_description"},
    )
    adapter.create_view("test_view", parse_one("SELECT a, b FROM tbl"), replace=False, materialized=True)

    assert to_sql_calls(adapter) == [
        "CREATE MATERIALIZED VIEW `test_view` (`a` COMMENT 'test_column_description', `b` COMMENT 'test_column_description') AS SELECT `a`, `b` FROM `tbl`",
        "CREATE MATERIALIZED VIEW `test_view` AS SELECT `a`, `b` FROM `tbl`",
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"unique_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"duplicate_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT")},
        table_description="test_description",
        column_descriptions={"a": "test_column_description"},
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') UNIQUE KEY (`a`)",
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') DUPLICATE KEY (`a`)",
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') COMMENT 'test_description'",
    ]

    adapter.cursor.reset_mock()


def test_create_table_like(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_table_like("target_table", "source_table")

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `target_table` LIKE `source_table`",
    ]


def test_create_schema(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema", ignore_if_exists=False)

    assert to_sql_calls(adapter) == [
        "CREATE DATABASE IF NOT EXISTS `test_schema`",
        "CREATE DATABASE `test_schema`",
    ]

    with pytest.raises(UnsupportedCatalogOperationError):
        adapter.create_schema("test_catalog.test_schema")


def test_drop_schema(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.drop_schema("test_schema")
    adapter.drop_schema("test_schema", ignore_if_not_exists=False)

    assert to_sql_calls(adapter) == [
        "DROP DATABASE IF EXISTS `test_schema`",
        "DROP DATABASE `test_schema`",
    ]


def test_rename_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.rename_table("old_table", "new_table")
    adapter.cursor.execute.assert_called_once_with("ALTER TABLE `old_table` RENAME `new_table`")


def test_replace_by_key(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    temp_table = parse_one("temp_table")
    mocker.patch.object(adapter, "_get_temp_table", return_value=temp_table)
    ctas_mock = mocker.patch.object(adapter, "ctas")
    delete_from_mock = mocker.patch.object(adapter, "delete_from")
    drop_table_mock = mocker.patch.object(adapter, "drop_table")

    columns_to_types = {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    source_query = parse_one("SELECT a, b FROM src")
    target_table = "target_table"

    # Single key, is_unique_key True and False
    for key, is_unique_key, expected_insert_sql in [
        (
            [exp.column("a")],
            True,
            "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a` ORDER BY `a`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        ),
        (
            [exp.column("a")],
            False,
            "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
        ),
        (
            [exp.column("a"), exp.column("b")],
            True,
            "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a`, `b` ORDER BY `a`, `b`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        ),
        (
            [exp.column("a"), exp.column("b")],
            False,
            "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
        ),
    ]:
        ctas_mock.reset_mock()
        delete_from_mock.reset_mock()
        drop_table_mock.reset_mock()
        adapter.cursor.execute.reset_mock()

        adapter._replace_by_key(target_table, source_query, columns_to_types, key, is_unique_key)

        ctas_mock.assert_called_once()
        drop_table_mock.assert_called_once_with(temp_table)
        # delete_from is only called if not is_replace_where (default)
        delete_from_mock.assert_called_once()
        sql_calls = to_sql_calls(adapter)
        assert any(expected_insert_sql in sql for sql in sql_calls), (
            f"Expected SQL not found: {expected_insert_sql}, get {sql_calls}"
        )


def test_create_index(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_index("test_table", "test_index", ("cola",))
    adapter.cursor.execute.assert_called_once_with("CREATE INDEX IF NOT EXISTS `test_index` ON `test_table`(`cola`)")


def test_create_table_with_distributed_by(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    distributed_by = {
        "expressions": ["a", "b"],
        "kind": "HASH",
        "buckets": 8,
    }
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY HASH (`a`, `b`) BUCKETS 8",
    ]

    adapter.cursor.execute.reset_mock()

    distributed_by = {
        "expressions": None,
        "kind": "RANDOM",
        "buckets": None,
    }
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY RANDOM",
    ]

    adapter.cursor.execute.reset_mock()

    distributed_by = {
        "expressions": ["a"],
        "kind": "HASH",
        "buckets": "AUTO",
    }
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY HASH (`a`) BUCKETS AUTO",
    ]


def test_create_table_with_properties(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT")},
        table_properties={
            "refresh_interval": "86400",
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT) PROPERTIES ('refresh_interval'='86400')",
    ]


def test_create_table_with_partitioned_by(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("DATE")},
        partitioned_by=[exp.to_column("b")],
        table_properties={"partitioned_by_expr": "FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` DATE) PARTITION BY RANGE (`b`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR)",
    ]

    adapter.cursor.execute.reset_mock()

    adapter.create_table(
        "test_table",
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("DATE")},
        partitioned_by=[exp.to_column("b")],
        table_properties={
            "partitioned_by_expr": [
                "PARTITION `p201701` VALUES [('2017-01-01'), ('2017-02-01'))",
                "PARTITION `other` VALUES LESS THAN (MAXVALUE)",
            ]
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` DATE) PARTITION BY RANGE (`b`) (PARTITION `p201701` VALUES [('2017-01-01'), ('2017-02-01')), PARTITION `other` VALUES LESS THAN (MAXVALUE))",
    ]


def test_create_full_materialized_view(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    materialized_properties = {
        "build": "IMMEDIATE",
        "refresh": "AUTO",
        "on_schedule": "EVERY 1 DAY STARTS '2024-12-01 20:30:00'",
        "distributed_by": {
            "kind": "HASH",
            "expressions": ["orderkey"],
            "buckets": 2,
        },
        "unique_key": ["orderkey"],
        "partitioned_by_expr": "FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR",
        "replication_num": "1",
    }
    columns_to_types = {
        "orderdate": exp.DataType.build("DATE"),
        "orderkey": exp.DataType.build("INT"),
        "partkey": exp.DataType.build("INT"),
    }
    column_descriptions = {
        "orderdate": "order date",
        "orderkey": "order key",
        "partkey": "part key",
    }
    query = parse_one(
        """
        SELECT 
        o_orderdate, 
        l_orderkey, 
        l_partkey 
        FROM 
        orders 
        LEFT JOIN lineitem ON l_orderkey = o_orderkey 
        LEFT JOIN partsupp ON ps_partkey = l_partkey 
        and l_suppkey = ps_suppkey
        """
    )
    adapter.create_view(
        "complete_mv",
        query,
        replace=False,
        materialized=True,
        columns_to_types=columns_to_types,
        column_descriptions=column_descriptions,
        table_description="test_description",
        materialized_properties=materialized_properties,
        partitioned_by=["orderdate"],
    )
    expected_sqls = [
        "CREATE MATERIALIZED VIEW `complete_mv` (`orderdate` COMMENT 'order date', `orderkey` COMMENT 'order key', `partkey` COMMENT 'part key') "
        "BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 DAY STARTS '2024-12-01 20:30:00' KEY (`orderkey`) COMMENT 'test_description' "
        "PARTITION BY RANGE (`orderdate`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR) "
        "DISTRIBUTED BY HASH (orderkey) BUCKETS 2 PROPERTIES ('replication_num'='1') "
        "AS SELECT `o_orderdate`, `l_orderkey`, `l_partkey` FROM `orders` LEFT JOIN `lineitem` ON `l_orderkey` = `o_orderkey` LEFT JOIN `partsupp` ON `ps_partkey` = `l_partkey` AND `l_suppkey` = `ps_suppkey`",
    ]
    sql_calls = to_sql_calls(adapter)

    # Remove extra spaces for comparison
    def norm(s):
        return " ".join(s.split())

    for expected_sql in expected_sqls:
        assert any(norm(expected_sql) == norm(sql) for sql in sql_calls), (
            f"Expected SQL not found.\nExpected: {expected_sql}\nGot: {sql_calls}"
        )


def test_create_table_with_single_string_distributed_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    """Test creating table with distributed_by where expressions is a single string."""
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    distributed_by = {
        "expressions": "recordid",  # Single string instead of array
        "kind": "HASH",
        "buckets": 10,
    }
    adapter.create_table(
        "test_table",
        columns_to_types={"recordid": exp.DataType.build("INT"), "name": exp.DataType.build("VARCHAR")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`recordid` INT, `name` VARCHAR) DISTRIBUTED BY HASH (`recordid`) BUCKETS 10",
    ]
