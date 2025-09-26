import typing as t

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from tests.core.engine_adapter import to_sql_calls

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from sqlmesh.utils.errors import UnsupportedCatalogOperationError

from pytest_mock.plugin import MockerFixture

pytestmark = [pytest.mark.doris, pytest.mark.engine]


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
        target_columns_to_types={"a": exp.DataType.build("INT")},
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
        target_columns_to_types={"a": exp.DataType.build("INT")},
    )
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        materialized=True,
        target_columns_to_types={"a": exp.DataType.build("INT")},
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
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description", "b": "test_column_description"},
    )
    adapter.create_view(
        "test_view", parse_one("SELECT a, b FROM tbl"), replace=False, materialized=True
    )

    assert to_sql_calls(adapter) == [
        "CREATE MATERIALIZED VIEW `test_view` (`a` COMMENT 'test_column_description', `b` COMMENT 'test_column_description') AS SELECT `a`, `b` FROM `tbl`",
        "CREATE MATERIALIZED VIEW `test_view` AS SELECT `a`, `b` FROM `tbl`",
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"unique_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"duplicate_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        table_description="test_description",
        column_descriptions={"a": "test_column_description"},
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') UNIQUE KEY (`a`) DISTRIBUTED BY HASH (`a`) BUCKETS 10",
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


def test_replace_by_key(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    temp_table = parse_one("temp_table")
    mocker.patch.object(adapter, "_get_temp_table", return_value=temp_table)
    mocker.patch.object(adapter, "ctas")
    mocker.patch.object(adapter, "delete_from")
    mocker.patch.object(adapter, "drop_table")

    columns_to_types = {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    source_query = parse_one("SELECT a, b FROM src")
    target_table = "target_table"

    adapter._replace_by_key(target_table, source_query, columns_to_types, [exp.column("a")], True)
    adapter._replace_by_key(target_table, source_query, columns_to_types, [exp.column("a")], False)
    adapter._replace_by_key(
        target_table, source_query, columns_to_types, [exp.column("a"), exp.column("b")], True
    )
    adapter._replace_by_key(
        target_table, source_query, columns_to_types, [exp.column("a"), exp.column("b")], False
    )

    assert to_sql_calls(adapter, identify=True) == [
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a` ORDER BY `a`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a`, `b` ORDER BY `a`, `b`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
    ]


def test_create_index(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_index("test_table", "test_index", ("cola",))
    adapter.cursor.execute.assert_called_once_with(
        "CREATE INDEX IF NOT EXISTS `test_index` ON `test_table`(`cola`)"
    )


def test_create_table_with_distributed_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    distributed_by = {
        "expressions": ["a", "b"],
        "kind": "HASH",
        "buckets": 8,
    }
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
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
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
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
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY HASH (`a`) BUCKETS AUTO",
    ]


def test_create_table_with_properties(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        table_properties={
            "refresh_interval": "86400",
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT) PROPERTIES ('refresh_interval'='86400')",
    ]


def test_create_table_with_partitioned_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("DATE")},
        partitioned_by=[exp.Literal.string("RANGE(b)")],
        table_properties={
            "partitions": exp.Literal.string(
                "FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR"
            )
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` DATE) PARTITION BY RANGE (`b`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR)",
    ]


def test_create_table_with_range_partitioned_by_with_partitions(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "waiter_id": exp.DataType.build("INT"),
            "customer_id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATETIME"),
        },
        partitioned_by=[exp.Literal.string("RANGE(ds)")],
        table_properties={
            "partitions": exp.Tuple(
                expressions=[
                    exp.Literal.string('PARTITION `p2023` VALUES [("2023-01-01"), ("2024-01-01"))'),
                    exp.Literal.string('PARTITION `p2024` VALUES [("2024-01-01"), ("2025-01-01"))'),
                    exp.Literal.string('PARTITION `p2025` VALUES [("2025-01-01"), ("2026-01-01"))'),
                    exp.Literal.string("PARTITION `other` VALUES LESS THAN MAXVALUE"),
                ]
            ),
            "distributed_by": exp.Tuple(
                expressions=[
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                        expression=exp.Literal.string("HASH"),
                    ),
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="expressions", quoted=True)),
                        expression=exp.Column(this=exp.Identifier(this="id", quoted=True)),
                    ),
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="buckets", quoted=True)),
                        expression=exp.Literal.number(10),
                    ),
                ]
            ),
            "replication_allocation": exp.Literal.string("tag.location.default: 3"),
            "in_memory": exp.Literal.string("false"),
            "storage_format": exp.Literal.string("V2"),
            "disable_auto_compaction": exp.Literal.string("false"),
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `waiter_id` INT, `customer_id` INT, `ds` DATETIME) "
        "PARTITION BY RANGE (`ds`) "
        '(PARTITION `p2023` VALUES [("2023-01-01"), ("2024-01-01")), '
        'PARTITION `p2024` VALUES [("2024-01-01"), ("2025-01-01")), '
        'PARTITION `p2025` VALUES [("2025-01-01"), ("2026-01-01")), '
        "PARTITION `other` VALUES LESS THAN MAXVALUE) "
        "DISTRIBUTED BY HASH (`id`) BUCKETS 10 "
        "PROPERTIES ("
        "'replication_allocation'='tag.location.default: 3', "
        "'in_memory'='false', "
        "'storage_format'='V2', "
        "'disable_auto_compaction'='false')"
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_table_with_list_partitioned_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "status": exp.DataType.build("VARCHAR(10)"),
        },
        partitioned_by=[exp.Literal.string("LIST(status)")],
        table_properties={
            "partitions": exp.Tuple(
                expressions=[
                    exp.Literal.string('PARTITION `active` VALUES IN ("active", "pending")'),
                    exp.Literal.string('PARTITION `inactive` VALUES IN ("inactive", "disabled")'),
                ]
            ),
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `status` VARCHAR(10)) "
        "PARTITION BY LIST (`status`) "
        '(PARTITION `active` VALUES IN ("active", "pending"), '
        'PARTITION `inactive` VALUES IN ("inactive", "disabled"))'
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_table_with_range_partitioned_by_anonymous_function(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    """Test that RANGE(ds) function call syntax generates correct SQL without duplicate RANGE."""
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATETIME"),
        },
        # This simulates how partitioned_by RANGE(ds) gets parsed from model definition
        partitioned_by=[exp.Anonymous(this="RANGE", expressions=[exp.to_column("ds")])],
        table_properties={
            "partitions": exp.Literal.string(
                'FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 1 MONTH'
            )
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `ds` DATETIME) "
        "PARTITION BY RANGE (`ds`) "
        '(FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 1 MONTH)'
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_materialized_view_with_duplicate_key(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_mv",
        parse_one("SELECT id, status, COUNT(*) as cnt FROM orders GROUP BY id, status"),
        materialized=True,
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "status": exp.DataType.build("VARCHAR(10)"),
            "cnt": exp.DataType.build("BIGINT"),
        },
        view_properties={
            "duplicate_key": exp.Tuple(
                expressions=[
                    exp.to_column("id"),
                    exp.to_column("status"),
                ]
            ),
        },
    )

    expected_sqls = [
        "DROP MATERIALIZED VIEW IF EXISTS `test_mv`",
        (
            "CREATE MATERIALIZED VIEW `test_mv` "
            "(`id`, `status`, `cnt`) "
            "DUPLICATE KEY (`id`, `status`) "
            "AS SELECT `id`, `status`, COUNT(*) AS `cnt` FROM `orders` GROUP BY `id`, `status`"
        ),
    ]

    assert to_sql_calls(adapter) == expected_sqls


def test_create_full_materialized_view(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    view_properties: t.Dict[str, exp.Expression] = {
        "build": exp.Literal.string("IMMEDIATE"),
        "refresh": exp.Tuple(
            expressions=[
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="method", quoted=True)),
                    expression=exp.Literal.string("AUTO"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                    expression=exp.Literal.string("SCHEDULE"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="every", quoted=True)),
                    expression=exp.Literal.number(1),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="unit", quoted=True)),
                    expression=exp.Literal.string("DAY"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="starts", quoted=True)),
                    expression=exp.Literal.string("2024-12-01 20:30:00"),
                ),
            ],
        ),
        "distributed_by": exp.Tuple(
            expressions=[
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                    expression=exp.Literal.string("HASH"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="expressions", quoted=True)),
                    expression=exp.Column(this=exp.Identifier(this="orderkey", quoted=True)),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="buckets", quoted=True)),
                    expression=exp.Literal.number(2),
                ),
            ]
        ),
        "unique_key": exp.to_column("orderkey"),
        "replication_num": exp.Literal.string("1"),
    }
    materialized_properties = {
        "partitioned_by": [
            parse_one("DATE_TRUNC(o_orderdate, 'MONTH')", dialect="doris"),
        ],
        "clustered_by": [],
        "partition_interval_unit": None,
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
        target_columns_to_types=columns_to_types,
        column_descriptions=column_descriptions,
        table_description="test_description",
        view_properties=view_properties,
        materialized_properties=materialized_properties,
    )
    expected_sqls = [
        "CREATE MATERIALIZED VIEW `complete_mv` (`orderdate` COMMENT 'order date', `orderkey` COMMENT 'order key', `partkey` COMMENT 'part key') "
        "BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 DAY STARTS '2024-12-01 20:30:00' KEY (`orderkey`) COMMENT 'test_description' "
        "PARTITION BY (DATE_TRUNC(`o_orderdate`, 'MONTH')) "
        "DISTRIBUTED BY HASH (`orderkey`) BUCKETS 2 PROPERTIES ('replication_num'='1') "
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
        target_columns_to_types={
            "recordid": exp.DataType.build("INT"),
            "name": exp.DataType.build("VARCHAR"),
        },
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`recordid` INT, `name` VARCHAR) DISTRIBUTED BY HASH (`recordid`) BUCKETS 10",
    ]


def test_delete_from_with_subquery(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with IN subquery
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e)",
    )

    # Test DELETE FROM with NOT IN subquery
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id NOT IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e)",
    )

    # Test simple DELETE FROM (should use base implementation)
    adapter.delete_from("test_schema_xtll416o.test_table", "id = 1")

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e`) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id`",
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e`) AS `_t2` WHERE `_t1`.`id` <> `_t2`.`id`",
        "DELETE FROM `test_schema_xtll416o`.`test_table` WHERE `id` = 1",
    ]


def test_delete_from_with_complex_subquery(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with complex subquery (multiple columns)
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "(id, name) IN (SELECT id, name FROM test_schema_xtll416o.temp_test_table_ik29031e WHERE active = 1)",
    )

    # Test DELETE FROM with subquery that has WHERE clause
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e WHERE created_date > '2024-01-01')",
    )

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id`, `name` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e` WHERE `active` = 1) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id` AND `_t1`.`name` = `_t2`.`name`",
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e` WHERE `created_date` > '2024-01-01') AS `_t2` WHERE `_t1`.`id` = `_t2`.`id`",
    ]


def test_delete_from_fallback_to_base(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with simple conditions (should use base implementation)
    adapter.delete_from("test_table", "id = 1")
    adapter.delete_from("test_table", "name = 'test' AND status = 'active'")
    adapter.delete_from("test_table", "id IN (1, 2, 3)")  # Simple IN with values, not subquery

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_table` WHERE `id` = 1",
        "DELETE FROM `test_table` WHERE `name` = 'test' AND `status` = 'active'",
        "DELETE FROM `test_table` WHERE `id` IN (1, 2, 3)",
    ]


def test_delete_from_full_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with WHERE TRUE (should use TRUNCATE)
    adapter.delete_from("test_table", exp.true())

    assert to_sql_calls(adapter) == [
        "TRUNCATE TABLE `test_table`",
    ]
