import logging
import typing as t
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
from sqlmesh.core.model.definition import ExternalModel
from sqlmesh.core.schema_loader import create_external_models_file
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
    # there was no explicit gateway, so it should not be written to the external models schema
    assert context.models['"memory"."sushi"."raw_fruits"'].gateway is None


def test_gateway_specific_external_models(tmpdir: Path):
    gateways = {
        "dev": GatewayConfig(connection=DuckDBConnectionConfig()),
        "prod": GatewayConfig(connection=DuckDBConnectionConfig()),
    }

    config = Config(gateways=gateways, default_gateway="dev")

    dev_context = Context(paths=[tmpdir], config=config, gateway="dev")
    dev_context.engine_adapter.execute("create schema landing")
    dev_context.engine_adapter.execute("create table landing.dev_source as select 1")
    dev_context.engine_adapter.execute("create schema lake")

    prod_context = Context(paths=[tmpdir], config=config, gateway="prod")
    prod_context.engine_adapter.execute("create schema landing")
    prod_context.engine_adapter.execute("create table landing.prod_source as select 1")
    prod_context.engine_adapter.execute("create schema lake")

    def _create_model(gateway: str):
        return load_sql_based_model(
            parse(
                """
            MODEL (
                name lake.table,
                kind FULL,
            );

            SELECT * FROM landing.@{gateway}_source
            """,
            ),
            variables={"gateway": gateway},
            default_catalog="memory",
        )

    dev_context.upsert_model(_create_model("dev"))
    prod_context.upsert_model(_create_model("prod"))

    dev_context.create_external_models()
    dev_context.load()

    dev_models = dev_context.models
    assert len(dev_models) == 1
    dev_model = t.cast(ExternalModel, dev_models['"memory"."landing"."dev_source"'])
    assert dev_model.gateway == "dev"

    prod_context.create_external_models()
    prod_context.load()
    prod_models = prod_context.models
    prod_model = t.cast(ExternalModel, prod_models['"memory"."landing"."prod_source"'])
    assert prod_model.gateway == "prod"

    # each context can only see models for its own gateway
    # check that models from both gateways present in the file, to show that prod_context.create_external_models() didnt clobber the dev ones
    external_models_filename = tmpdir / c.EXTERNAL_MODELS_YAML
    with open(external_models_filename, "r", encoding="utf8") as fd:
        contents = YAML().load(fd)

        assert len(contents) == 2
        assert len([c for c in contents if c["name"] == '"memory"."landing"."dev_source"']) == 1
        assert len([c for c in contents if c["name"] == '"memory"."landing"."prod_source"']) == 1


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

    filename = tmp_path / c.EXTERNAL_MODELS_YAML
    create_external_models_file(
        filename,
        {  # type: ignore
            "a": model_a,
            "b": model_b,
        },
        engine_adapter_mock,
        state_reader_mock,
        "bigquery",
    )

    with open(filename, "r", encoding="utf8") as fd:
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

    filename = tmp_path / c.EXTERNAL_MODELS_YAML
    logger = logging.getLogger("sqlmesh.core.schema_loader")
    with patch.object(logger, "warning") as mock_logger:
        create_external_models_file(
            filename,
            {"a": model},  # type: ignore
            context.engine_adapter,
            context.state_reader,
            "",
        )
    assert """Unable to get schema for '"tbl_source"'""" in mock_logger.call_args[0][0]

    with open(filename, "r", encoding="utf8") as fd:
        schema = YAML().load(fd)
    assert len(schema) == 0
