# type: ignore
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import EngineAdapter, SparkEngineAdapter


def test_create_view(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)

    assert cursor_mock.execute.mock_calls == [
        call("CREATE OR REPLACE VIEW `test_view` AS SELECT `a` FROM `tbl`"),
        call("CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`"),
    ]


def test_create_schema(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema", ignore_if_exists=False)

    assert cursor_mock.execute.mock_calls == [
        call("CREATE SCHEMA IF NOT EXISTS `test_schema`"),
        call("CREATE SCHEMA `test_schema`"),
    ]


def test_columns(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()

    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchall.return_value = [
        ("id", "int"),
        ("name", "string"),
        ("price", "double"),
        ("ds", "string"),
        ("# Partition Information", ""),
        ("# col_name", "data_type"),
        ("ds", "string"),
    ]

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    assert adapter.columns("test_table") == {
        "id": "INT",
        "name": "STRING",
        "price": "DOUBLE",
        "ds": "STRING",
    }

    cursor_mock.execute.assert_called_once_with(
        "DESCRIBE TABLE test_table",
    )


def test_table_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    assert adapter.table_exists("test_table")
    cursor_mock.execute.assert_called_once_with(
        "DESCRIBE TABLE test_table",
    )

    cursor_mock = mocker.Mock()
    cursor_mock.execute.side_effect = RuntimeError("error")
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    assert not adapter.table_exists("test_table")
    cursor_mock.execute.assert_called_once_with(
        "DESCRIBE TABLE test_table",
    )


def test_insert_overwrite(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.insert_overwrite(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        columns={"a": exp.DataType.build("INT")},
    )

    cursor_mock.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT `a` FROM `tbl`"
    )


def test_insert_append(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.insert_append(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        columns={"a": exp.DataType.build("INT")},
    )

    cursor_mock.execute.assert_called_once_with(
        "INSERT INTO `test_table` (`a`) SELECT `a` FROM `tbl`"
    )


def test_delete_insert_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.delete_insert_query(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        parse_one("a BETWEEN 0 and 1"),
        columns={"a": exp.DataType.build("INT")},
    )

    cursor_mock.execute.assert_has_calls(
        [
            mocker.call("DELETE FROM `test_table` WHERE `a` BETWEEN 0 AND 1"),
            mocker.call("INSERT INTO `test_table` (`a`) SELECT `a` FROM `tbl`"),
        ]
    )


def test_create_and_insert(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.create_and_insert(
        "test_table",
        {"a": exp.DataType.build("bigint")},
        parse_one("SELECT a::bigint AS a FROM tbl"),
    )

    cursor_mock.execute.mock_calls == [
        "CREATE TABLE IF NOT EXISTS test_table (a LONG)",
        "INSERT OVERWRITE TABLE test_table (`a`) SELECT CAST(a AS LONG) AS a FROM tbl",
    ]


def test_create_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    column_mapping = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.create_table("test_table", column_mapping)

    cursor_mock.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING)"
    )


def test_create_table_properties(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    column_mapping = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.create_table(
        "test_table",
        column_mapping,
        partitioned_by=["colb"],
        storage_format="ICEBERG",
    )

    cursor_mock.execute.assert_called_once_with(
        """CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING) USING ICEBERG PARTITIONED BY (`colb`)"""
    )


def test_create_table_properties_ignored(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    column_mapping = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapter(lambda: connection_mock, "duckdb")  # type: ignore
    adapter.create_table(
        "test_table",
        column_mapping,
        partitioned_by=["colb"],
        storage_format="ICEBERG",
    )

    cursor_mock.execute.assert_called_once_with(
        """CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT)"""
    )


def test_alter_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "duckdb")  # type: ignore
    adapter.alter_table(
        "test_table",
        {"a": "INT", "b": "TEXT"},
        ["c", "d"],
    )

    cursor_mock.execute.assert_has_calls(
        [
            call("BEGIN"),
            call("""ALTER TABLE test_table ADD COLUMN a INT"""),
            call("""ALTER TABLE test_table ADD COLUMN b TEXT"""),
            call("""ALTER TABLE test_table DROP COLUMN c"""),
            call("""ALTER TABLE test_table DROP COLUMN d"""),
            call("COMMIT"),
        ]
    )


def test_alter_table_spark(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = SparkEngineAdapter(lambda: connection_mock)  # type: ignore
    adapter.alter_table(
        "test_table",
        {"a": "INT", "b": "STRING"},
        ["c", "d"],
    )

    cursor_mock.execute.assert_has_calls(
        [
            call("""ALTER TABLE test_table ADD COLUMNS (a INT, b STRING)"""),
            call("""ALTER TABLE test_table DROP COLUMNS (c, d)"""),
        ]
    )


@pytest.fixture
def adapter(duck_conn):
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")
    return EngineAdapter(lambda: duck_conn, "duckdb")


def test_create_view_duckdb(adapter: EngineAdapter, duck_conn):
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    assert duck_conn.execute("SELECT * FROM test_view").fetchall() == [(1,)]

    with pytest.raises(Exception):
        adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)


def test_create_schema_duckdb(adapter: EngineAdapter, duck_conn):
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema")
    assert duck_conn.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'test_schema'"
    ).fetchall() == [(1,)]
    with pytest.raises(Exception):
        adapter.create_schema("test_schema", ignore_if_exists=False)


def test_table_exists_duckdb(adapter: EngineAdapter, duck_conn):
    assert not adapter.table_exists("test_table")
    assert adapter.table_exists("tbl")


def test_create_and_insert_duckdb(adapter: EngineAdapter, duck_conn):
    adapter.create_and_insert(
        "test_table",
        {"a": exp.DataType.build("int")},
        parse_one("SELECT a::int AS a FROM tbl"),
    )
    assert duck_conn.execute("SELECT * FROM test_table").fetchall() == [(1,)]
    adapter.create_and_insert(
        "test_table",
        {"a": exp.DataType.build("int")},
        parse_one("SELECT a::int AS a FROM tbl"),
    )
    assert duck_conn.execute("SELECT * FROM test_table").fetchall() == [(1,), (1,)]


def test_create_table_duckdb(adapter: EngineAdapter, duck_conn):
    column_mapping = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    expected_columns = [
        ("cola", "INTEGER", "YES", None, None, None),
        ("colb", "VARCHAR", "YES", None, None, None),
    ]
    adapter.create_table("test_table", column_mapping)
    assert duck_conn.execute("DESCRIBE test_table").fetchall() == expected_columns
    adapter.create_table(
        "test_table2",
        column_mapping,
        storage_format="ICEBERG",
        partitioned_by=["colb"],
    )
    assert duck_conn.execute("DESCRIBE test_table").fetchall() == expected_columns


def test_transaction_duckdb(adapter: EngineAdapter, duck_conn):
    adapter.create_table("test_table", {"a": exp.DataType.build("int")})
    with adapter.transaction():
        adapter.execute("INSERT INTO test_table (a) VALUES (1)")
    assert duck_conn.execute("SELECT * FROM test_table").fetchall() == [(1,)]

    # Assert transaction was rolled back if an exception was raised
    try:
        with adapter.transaction():
            adapter.execute("INSERT INTO test_table (a) VALUES (1)")
            raise Exception
    except Exception:
        pass
    assert duck_conn.execute("SELECT * FROM test_table").fetchall() == [(1,)]


def test_merge(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "spark")  # type: ignore
    adapter.merge(
        target_table="target",
        source_table="source",
        columns=["id", "ts", "val"],
        unique_keys=["id"],
    )
    cursor_mock.execute.assert_called_once_with(
        "MERGE INTO target USING source ON `target`.`id` = `source`.`id` "
        "WHEN MATCHED THEN UPDATE SET `target`.`id` = `source`.`id`, `target`.`ts` = `source`.`ts`, `target`.`val` = `source`.`val` "
        "WHEN NOT MATCHED THEN INSERT (`id`, `ts`, `val`) VALUES (`source`.`id`, `source`.`ts`, `source`.`val`)"
    )

    cursor_mock.reset_mock()
    adapter.merge(
        target_table="target",
        source_table="source",
        columns=["id", "ts", "val"],
        unique_keys=["id", "ts"],
    )
    cursor_mock.execute.assert_called_once_with(
        "MERGE INTO target USING source ON `target`.`id` = `source`.`id` AND `target`.`ts` = `source`.`ts` "
        "WHEN MATCHED THEN UPDATE SET `target`.`id` = `source`.`id`, `target`.`ts` = `source`.`ts`, `target`.`val` = `source`.`val` "
        "WHEN NOT MATCHED THEN INSERT (`id`, `ts`, `val`) VALUES (`source`.`id`, `source`.`ts`, `source`.`val`)"
    )
