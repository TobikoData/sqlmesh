import os
from pathlib import Path

from sqlmesh.core.config import DuckDBConnectionConfig
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.errors import ConfigError

DATA_DIR = os.path.join(os.path.dirname(__file__), "source_data")


def _add_csv_file(conn: EngineAdapter, table_name: str, path: str) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM READ_CSV_AUTO('{path}')"
    )


def init_raw_schema(conn: EngineAdapter) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    _add_csv_file(conn, "raw.items", f"{DATA_DIR}/items.csv")
    _add_csv_file(conn, "raw.orders", f"{DATA_DIR}/orders.csv")
    _add_csv_file(conn, "raw.order_items", f"{DATA_DIR}/order_items.csv")


profile = Profile.load(DbtContext(project_root=Path(__file__).parent))
connection_config = profile.target.to_sqlmesh()
if not isinstance(connection_config, DuckDBConnectionConfig):
    raise ConfigError(
        f"Only duckdb supported. The CSV files in {str(DATA_DIR)} must be manually loaded into your target"
    )

engine_adapter = connection_config.create_engine_adapter()
init_raw_schema(engine_adapter)
