"""Integration tests for GizmoSQL engine adapter.

These tests require a running GizmoSQL server with DuckDB backend.
They are marked with the 'gizmosql' and 'docker' pytest markers.
"""

import typing as t

import pytest
from sqlglot import exp

from sqlmesh.core.config.connection import GizmoSQLConnectionConfig
from sqlmesh.core.engine_adapter.gizmosql import GizmoSQLEngineAdapter

pytestmark = [pytest.mark.gizmosql, pytest.mark.engine, pytest.mark.docker]


@pytest.fixture(scope="session")
def gizmosql_config() -> GizmoSQLConnectionConfig:
    """Create a GizmoSQL connection config for testing.

    Environment variables can override defaults:
    - GIZMOSQL_HOST: hostname (default: localhost)
    - GIZMOSQL_PORT: port (default: 31337)
    - GIZMOSQL_USERNAME: username (default: gizmosql_username)
    - GIZMOSQL_PASSWORD: password (default: gizmosql_password)
    """
    import os

    return GizmoSQLConnectionConfig(
        host=os.environ.get("GIZMOSQL_HOST", "localhost"),
        port=int(os.environ.get("GIZMOSQL_PORT", "31337")),
        username=os.environ.get("GIZMOSQL_USERNAME", "gizmosql_username"),
        password=os.environ.get("GIZMOSQL_PASSWORD", "gizmosql_password"),
        use_encryption=True,
        disable_certificate_verification=True,
    )


@pytest.fixture(scope="session")
def gizmosql_adapter(gizmosql_config: GizmoSQLConnectionConfig) -> t.Generator[GizmoSQLEngineAdapter, None, None]:
    """Create a GizmoSQL engine adapter for testing."""
    adapter = gizmosql_config.create_engine_adapter()
    yield adapter
    adapter.close()


def test_connection(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test that we can connect and execute a simple query."""
    cursor = gizmosql_adapter.connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result[0] == 1


def test_dialect(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test that the adapter uses the DuckDB dialect."""
    assert gizmosql_adapter.dialect == "duckdb"


def test_create_and_drop_schema(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test creating and dropping schemas."""
    schema_name = "test_gizmosql_schema"

    # Clean up if exists
    gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)

    # Create schema
    gizmosql_adapter.create_schema(schema_name)

    # Verify it exists by selecting from information_schema
    result = gizmosql_adapter.fetchone(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'"
    )
    assert result is not None
    assert result[0] == schema_name

    # Drop schema
    gizmosql_adapter.drop_schema(schema_name, cascade=True)

    # Verify it's gone
    result = gizmosql_adapter.fetchone(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'"
    )
    assert result is None


def test_create_table_and_insert(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test creating a table and inserting data."""
    schema_name = "test_gizmosql_table_schema"
    table_name = f"{schema_name}.test_table"

    try:
        # Setup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)
        gizmosql_adapter.create_schema(schema_name)

        # Create table
        columns_to_types = {
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("VARCHAR"),
            "value": exp.DataType.build("DOUBLE"),
        }
        gizmosql_adapter.create_table(table_name, columns_to_types)

        # Insert data using SQL
        gizmosql_adapter.execute(
            f"INSERT INTO {table_name} (id, name, value) VALUES (1, 'test', 3.14)"
        )

        # Query data
        result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
        assert result is not None
        assert result[0] == 1
        assert result[1] == "test"
        assert abs(result[2] - 3.14) < 0.001

    finally:
        # Cleanup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)


def test_fetchdf(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test fetching results as a pandas DataFrame."""
    import pandas as pd

    df = gizmosql_adapter.fetchdf("SELECT 1 as a, 2 as b, 'hello' as c")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert list(df.columns) == ["a", "b", "c"]
    assert df["a"].iloc[0] == 1
    assert df["b"].iloc[0] == 2
    assert df["c"].iloc[0] == "hello"


def test_fetchall(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test fetchall returns list of tuples."""
    result = gizmosql_adapter.fetchall("SELECT 1 as a, 2 as b UNION ALL SELECT 3, 4")

    assert len(result) == 2


def test_fetchone(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test fetchone returns a single row."""
    result = gizmosql_adapter.fetchone("SELECT 42 as answer, 'hello' as greeting")

    assert result is not None
    assert result[0] == 42
    assert result[1] == "hello"


def test_table_exists(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test checking if a table exists."""
    schema_name = "test_gizmosql_exists_schema"
    table_name = f"{schema_name}.test_exists_table"

    try:
        # Setup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)
        gizmosql_adapter.create_schema(schema_name)

        # Table should not exist yet
        assert not gizmosql_adapter.table_exists(exp.to_table(table_name))

        # Create table
        columns_to_types = {"id": exp.DataType.build("INT")}
        gizmosql_adapter.create_table(table_name, columns_to_types)

        # Table should exist now
        assert gizmosql_adapter.table_exists(exp.to_table(table_name))

    finally:
        # Cleanup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)


def test_get_current_catalog(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test getting the current catalog."""
    catalog = gizmosql_adapter.get_current_catalog()
    # GizmoSQL should return a catalog name
    assert catalog is not None
    assert isinstance(catalog, str)


def test_get_current_schema(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test getting the current schema via SQL."""
    result = gizmosql_adapter.fetchone("SELECT current_schema()")
    assert result is not None
    assert isinstance(result[0], str)


def test_information_schema_access(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test that we can query information_schema."""
    result = gizmosql_adapter.fetchall(
        "SELECT schema_name FROM information_schema.schemata LIMIT 5"
    )
    assert result is not None
    assert len(result) > 0


def test_complex_query(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test a more complex analytical query with CTEs and aggregations."""
    query = """
    WITH numbers AS (
        SELECT unnest(ARRAY[1, 2, 3, 4, 5]) as n
    )
    SELECT
        SUM(n) as total,
        AVG(n) as average,
        MIN(n) as minimum,
        MAX(n) as maximum
    FROM numbers
    """
    result = gizmosql_adapter.fetchone(query)

    assert result is not None
    assert result[0] == 15  # sum
    assert result[1] == 3.0  # avg
    assert result[2] == 1  # min
    assert result[3] == 5  # max


def test_fetchdf_with_types(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test fetching DataFrame with various data types."""
    import pandas as pd

    query = """
    SELECT
        1::INTEGER as int_col,
        3.14::DOUBLE as double_col,
        'test'::VARCHAR as varchar_col,
        true::BOOLEAN as bool_col
    """
    df = gizmosql_adapter.fetchdf(query)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["int_col"].iloc[0] == 1
    assert abs(df["double_col"].iloc[0] - 3.14) < 0.001
    assert df["varchar_col"].iloc[0] == "test"
    assert df["bool_col"].iloc[0] == True  # noqa: E712 - numpy bool comparison


def test_query_with_expressions(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test querying using SQLGlot expressions."""
    from sqlglot import select, exp

    query = select(
        exp.Literal.number(1).as_("a"),
        exp.Literal.string("hello").as_("b"),
    )

    result = gizmosql_adapter.fetchone(query)
    assert result is not None
    assert result[0] == 1
    assert result[1] == "hello"


def test_dataframe_bulk_ingestion(gizmosql_adapter: GizmoSQLEngineAdapter):
    """Test bulk DataFrame ingestion using ADBC adbc_ingest."""
    import pandas as pd

    schema_name = "test_bulk_ingest_schema"
    table_name = f"{schema_name}.bulk_test_table"

    try:
        # Setup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)
        gizmosql_adapter.create_schema(schema_name)

        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "charlie", "diana", "eve"],
            "value": [10.5, 20.5, 30.5, 40.5, 50.5],
        })

        # Create target table
        columns_to_types = {
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("VARCHAR"),
            "value": exp.DataType.build("DOUBLE"),
        }
        gizmosql_adapter.create_table(table_name, columns_to_types)

        # Use replace_query with DataFrame (this uses _df_to_source_queries internally)
        gizmosql_adapter.replace_query(
            table_name,
            df,
            columns_to_types,
        )

        # Verify data was loaded
        result = gizmosql_adapter.fetchall(f"SELECT * FROM {table_name} ORDER BY id")
        assert len(result) == 5
        assert result[0][0] == 1
        assert result[0][1] == "alice"
        assert abs(result[0][2] - 10.5) < 0.001
        assert result[4][0] == 5
        assert result[4][1] == "eve"

    finally:
        # Cleanup
        gizmosql_adapter.drop_schema(schema_name, ignore_if_not_exists=True, cascade=True)
