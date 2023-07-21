import pathlib

from sqlmesh.core.config import Config
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_sql_based_model
from tests.utils.test_filesystem import create_temp_file


def test_format_files(tmp_path: pathlib.Path):
    models_dir = pathlib.Path("models")

    f1 = create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "model_1.sql"),
        "MODEL(name this.model, dialect 'duckdb'); SELECT 1 AS \"CaseSensitive\"",
    )
    f2 = create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "model_2.sql"),
        "MODEL(name other.model); SELECT 2 AS another_column",
    )

    config = Config()
    context = Context(paths=str(tmp_path), config=config)
    context.load()
    assert isinstance(context.models["this.model"], SqlModel)
    assert isinstance(context.models["other.model"], SqlModel)
    assert context.models["this.model"].query.sql() == 'SELECT 1 AS "CaseSensitive"'
    assert context.models["other.model"].query.sql() == "SELECT 2 AS another_column"

    # Transpile project to BigQuery
    context.format(transpile="bigquery")

    # Ensure transpilation success AND model specific dialect is mutated
    upd1 = f1.read_text(encoding="utf-8")
    assert (
        upd1
        == "MODEL (\n  name this.model,\n  dialect 'bigquery'\n);\n\nSELECT\n  1 AS `CaseSensitive`"
    )
    context.upsert_model(load_sql_based_model(parse(upd1, "bigquery")))
    assert context.models["this.model"].dialect == "bigquery"

    # Ensure no dialect is added if it's not needed
    upd2 = f2.read_text(encoding="utf-8")
    assert upd2 == "MODEL (\n  name other.model\n);\n\nSELECT\n  2 AS another_column"
