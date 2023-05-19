import os

import pandas as pd
import pytest
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import load_model
from sqlmesh.core.snapshot import SnapshotChangeCategory


@pytest.fixture(autouse=True)
def cleanup(sushi_context):
    yield
    os.remove(sushi_context.path / c.SCHEMA_YAML)


def test_create_external_models(sushi_context, assert_exp_eq):
    fruits = pd.DataFrame(
        [
            {"id": 1, "name": "apple"},
            {"id": 2, "name": "banana"},
        ]
    )

    cursor = sushi_context.engine_adapter.cursor
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

    sushi_context.upsert_model(model)
    sushi_context.create_external_models()
    assert sushi_context.models["sushi.fruits"].columns_to_types == {
        "name": exp.DataType.build("UNKNOWN")
    }
    sushi_context.load()

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

    sushi_context.upsert_model(model)
    raw_fruits = sushi_context.models["sushi.raw_fruits"]
    assert raw_fruits.kind.is_symbolic
    assert raw_fruits.kind.is_external
    assert raw_fruits.columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }

    snapshot = sushi_context.snapshots["sushi.raw_fruits"]
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    fruits = sushi_context.models["sushi.fruits"]
    assert not fruits.kind.is_symbolic
    assert not fruits.kind.is_external
    assert fruits.columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }
    assert_exp_eq(
        fruits.render_query(snapshots=sushi_context.snapshots),
        """
        SELECT
          raw_fruits.id AS id,
          raw_fruits.name AS name
        FROM sushi.raw_fruits AS raw_fruits
        """,
    )
