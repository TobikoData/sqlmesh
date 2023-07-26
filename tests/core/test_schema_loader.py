from pathlib import Path

import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core import constants as c
from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.schema_loader import create_schema_file
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.yaml import YAML


def test_create_external_models(tmpdir, assert_exp_eq):
    config = Config(gateways=GatewayConfig(connection=DuckDBConnectionConfig()))
    context = Context(paths=[tmpdir], config=config)

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
        )
    )

    context.upsert_model(model)
    context.create_external_models()
    assert context.models["sushi.fruits"].columns_to_types == {
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
        )
    )

    context.upsert_model(model)
    raw_fruits = context.models["sushi.raw_fruits"]
    assert raw_fruits.kind.is_symbolic
    assert raw_fruits.kind.is_external
    assert raw_fruits.columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }

    snapshot = context.snapshots["sushi.raw_fruits"]
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    fruits = context.models["sushi.fruits"]
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
        FROM "sushi"."raw_fruits" AS "raw_fruits"
        """,
    )
    # rerunning create external models should refetch existing external models
    context.create_external_models()
    context.load()
    assert context.models["sushi.raw_fruits"]


def test_no_internal_model_conversion(tmp_path: Path, mocker: MockerFixture):
    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.return_value = {
        "b": exp.DataType.build("text"),
        "a": exp.DataType.build("bigint"),
    }

    state_reader_mock = mocker.Mock()
    state_reader_mock.nodes_exist.return_value = {"model_b"}

    model = SqlModel(name="a", query=parse_one("select * FROM model_b, tbl_c"))

    schema_file = tmp_path / c.SCHEMA_YAML
    create_schema_file(schema_file, {"a": model}, engine_adapter_mock, state_reader_mock, "")

    with open(schema_file, "r") as fd:
        schema = YAML().load(fd)

    assert len(schema) == 1
    assert schema[0]["name"] == "tbl_c"
    assert list(schema[0]["columns"]) == ["b", "a"]
