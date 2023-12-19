import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core import constants as c
from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, create_external_model, load_sql_based_model
from sqlmesh.core.schema_loader import create_schema_file
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.yaml import YAML


def test_create_external_models(tmpdir, assert_exp_eq):
    config = Config(gateways=GatewayConfig(connection=DuckDBConnectionConfig()))
    context = Context(paths=[tmpdir], config=config)

    # `fruits` is used by DuckDB in the upcoming select query
    fruits = pd.DataFrame(
        [
            {"id": 1, "name": "apple"},
            {"id": 2, "name": "banana"},
        ]
    )

    cursor = context.engine_adapter.cursor
    cursor.execute("CREATE SCHEMA sushi")
    cursor.execute("CREATE TABLE sushi.raw_fruits AS SELECT * FROM fruits")

    model = load_sql_based_model(
        parse(
            """
        MODEL (
            name sushi.fruits,
            kind FULL,
        );

        SELECT name FROM sushi.raw_fruits
    """,
        ),
        default_catalog="memory",
    )

    context.upsert_model(model)
    context.create_external_models()
    assert context.models['"memory"."sushi"."fruits"'].columns_to_types == {
        "name": exp.DataType.build("UNKNOWN")
    }
    context.load()

    model = load_sql_based_model(
        parse(
            """
        MODEL (
            name sushi.fruits,
            kind FULL,
        );

        SELECT * FROM sushi.raw_fruits
    """,
        ),
        default_catalog="memory",
    )

    context.upsert_model(model)
    raw_fruits = context.models['"memory"."sushi"."raw_fruits"']
    assert raw_fruits.kind.is_symbolic
    assert raw_fruits.kind.is_external
    assert raw_fruits.columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }

    snapshot = context.get_snapshot("sushi.raw_fruits", raise_if_missing=True)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    fruits = context.models['"memory"."sushi"."fruits"']
    assert not fruits.kind.is_symbolic
    assert not fruits.kind.is_external
    assert fruits.columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }
    assert_exp_eq(
        fruits.render_query(snapshots=context.snapshots),
        """
        SELECT
          "raw_fruits"."id" AS "id",
          "raw_fruits"."name" AS "name"
        FROM "memory"."sushi"."raw_fruits" AS "raw_fruits"
        """,
    )
    # rerunning create external models should refetch existing external models
    context.create_external_models()
    context.load()
    assert context.models['"memory"."sushi"."raw_fruits"']


def test_no_internal_model_conversion(tmp_path: Path, make_snapshot, mocker: MockerFixture):
    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.return_value = {
        "b": exp.DataType.build("text"),
        "a": exp.DataType.build("bigint"),
    }

    state_reader_mock = mocker.Mock()
    state_reader_mock.nodes_exist.return_value = {'"model_b"'}

    model_a = SqlModel(name="a", query=parse_one("select * FROM model_b, tbl_c"))
    model_b = SqlModel(name="b", query=parse_one("select * FROM `tbl-d`", read="bigquery"))

    schema_file = tmp_path / c.SCHEMA_YAML
    create_schema_file(
        schema_file,
        {  # type: ignore
            "a": model_a,
            "b": model_b,
        },
        engine_adapter_mock,
        state_reader_mock,
        "bigquery",
    )

    with open(schema_file, "r") as fd:
        schema = YAML().load(fd)

    assert len(schema) == 2
    assert schema[0]["name"] == "`tbl-d`"
    assert list(schema[0]["columns"]) == ["b", "a"]
    assert schema[1]["name"] == "`tbl_c`"
    assert list(schema[1]["columns"]) == ["b", "a"]

    for row in schema:
        create_external_model(**row, dialect="bigquery")


def test_missing_table(tmp_path: Path):
    config = Config(gateways=GatewayConfig(connection=DuckDBConnectionConfig()))
    context = Context(paths=[str(tmp_path.absolute())], config=config)
    model = SqlModel(name="a", query=parse_one("select * FROM tbl_source"))

    schema_file = tmp_path / c.SCHEMA_YAML
    logger = logging.getLogger("sqlmesh.core.schema_loader")
    with patch.object(logger, "warning") as mock_logger:
        create_schema_file(
            schema_file, {"a": model}, context.engine_adapter, context.state_reader, ""  # type: ignore
        )
    assert """Unable to get schema for '"tbl_source"'""" in mock_logger.call_args[0][0]

    with open(schema_file, "r") as fd:
        schema = YAML().load(fd)
    assert len(schema) == 0
