import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import DuckDBEngineAdapter, EngineAdapter


@pytest.fixture
def adapter(duck_conn):
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")
    return DuckDBEngineAdapter(lambda: duck_conn)


def test_create_view(adapter: EngineAdapter, duck_conn):
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))  # type: ignore
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))  # type: ignore
    assert duck_conn.execute("SELECT * FROM test_view").fetchall() == [(1,)]

    with pytest.raises(Exception):
        adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)  # type: ignore


def test_create_schema(adapter: EngineAdapter, duck_conn):
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema")
    assert duck_conn.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'test_schema'"
    ).fetchall() == [(1,)]
    with pytest.raises(Exception):
        adapter.create_schema("test_schema", ignore_if_exists=False)


def test_table_exists(adapter: EngineAdapter, duck_conn):
    assert not adapter.table_exists("test_table")
    assert adapter.table_exists("tbl")


def test_create_table(adapter: EngineAdapter, duck_conn):
    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    expected_columns = [
        ("cola", "INTEGER", "YES", None, None, None),
        ("colb", "VARCHAR", "YES", None, None, None),
    ]
    adapter.create_table("test_table", columns_to_types)
    assert duck_conn.execute("DESCRIBE test_table").fetchall() == expected_columns
    adapter.create_table(
        "test_table2",
        columns_to_types,
        storage_format="ICEBERG",
        partitioned_by=["colb"],
    )
    assert duck_conn.execute("DESCRIBE test_table").fetchall() == expected_columns


def test_transaction(adapter: EngineAdapter, duck_conn):
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
