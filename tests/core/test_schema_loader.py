import pandas as pd
from sqlglot import exp

from sqlmesh.core.config import Config, DuckDBConnectionConfig, GatewayConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import load_model
from sqlmesh.core.snapshot import SnapshotChangeCategory


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

    model = load_model(
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

    model = load_model(
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
          raw_fruits.id AS id,
          raw_fruits.name AS name
        FROM sushi.raw_fruits AS raw_fruits
        """,
    )
