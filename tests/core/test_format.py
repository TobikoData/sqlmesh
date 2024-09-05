import pathlib

from sqlmesh.core.config import Config
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.audit import ModelAudit
from sqlmesh.core.model import SqlModel, load_sql_based_model
from tests.utils.test_filesystem import create_temp_file


def test_format_files(tmp_path: pathlib.Path):
    models_dir = pathlib.Path("models")
    audits_dir = pathlib.Path("audits")

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
    f3 = create_temp_file(
        tmp_path,
        pathlib.Path(audits_dir, "audit_1.sql"),
        "AUDIT(name assert_positive_id, dialect 'duckdb'); SELECT  * FROM @this_model WHERE  \"CaseSensitive\"_item_id < 0;",
    )
    f4 = create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "model_3.sql"),
        "MODEL(name audit.model, audits (inline_audit)); SELECT 3 AS item_id; AUDIT(name inline_audit); SELECT  * FROM @this_model WHERE  item_id < 0;",
    )

    config = Config()
    context = Context(paths=tmp_path, config=config)
    context.load()

    assert isinstance(context.get_model("this.model"), SqlModel)
    assert isinstance(context.get_model("other.model"), SqlModel)
    assert isinstance(context.get_model("audit.model"), SqlModel)
    assert isinstance(context._audits["assert_positive_id"], ModelAudit)
    assert context.get_model("this.model").query.sql() == 'SELECT 1 AS "CaseSensitive"'  # type: ignore
    assert context.get_model("other.model").query.sql() == "SELECT 2 AS another_column"  # type: ignore
    assert context.get_model("audit.model").query.sql() == "SELECT 3 AS item_id"  # type: ignore
    assert (
        context.get_model("audit.model").inline_audits["inline_audit"].query.sql()
        == "SELECT * FROM @this_model WHERE item_id < 0"
    )
    assert (
        context._audits["assert_positive_id"].query.sql()
        == 'SELECT * FROM @this_model WHERE "CaseSensitive_item_id" < 0'
    )

    assert not context.format(check=True)

    # Transpile project to BigQuery
    context.format(transpile="bigquery")

    # Ensure format check is successful
    assert context.format(transpile="bigquery", check=True)

    # Ensure transpilation success AND model specific dialect is mutated
    upd1 = f1.read_text(encoding="utf-8")
    assert (
        upd1
        == "MODEL (\n  name this.model,\n  dialect 'bigquery'\n);\n\nSELECT\n  1 AS `CaseSensitive`"
    )
    context.upsert_model(load_sql_based_model(parse(upd1, "bigquery"), default_catalog="memory"))
    assert context.models['"memory"."this"."model"'].dialect == "bigquery"

    # Ensure no dialect is added if it's not needed
    upd2 = f2.read_text(encoding="utf-8")
    assert upd2 == "MODEL (\n  name other.model\n);\n\nSELECT\n  2 AS another_column"

    # Ensure audit specific dialect is updated and formatting
    upd3 = f3.read_text(encoding="utf-8")
    assert (
        upd3
        == "AUDIT (\n  name assert_positive_id,\n  dialect 'bigquery'\n);\n\nSELECT\n  *\nFROM @this_model\nWHERE\n  `CaseSensitive_item_id` < 0"
    )

    # Ensure inline audit is formatted within model definition
    upd4 = f4.read_text(encoding="utf-8")
    assert (
        upd4
        == "MODEL (\n  name audit.model,\n  audits (\n    inline_audit\n  )\n);\n\nSELECT\n  3 AS item_id;\n\nAUDIT (\n  name inline_audit\n);\n\nSELECT\n  *\nFROM @this_model\nWHERE\n  item_id < 0"
    )
