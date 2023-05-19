import os

import pandas as pd
import pytest
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import load_model


@pytest.fixture(autouse=True)
def cleanup(sushi_context):
    yield
    os.remove(sushi_context.path / c.SCHEMA_YAML)


def test_create_external_models(sushi_context):
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
    assert sushi_context.models["sushi.raw_fruits"].columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }

    assert sushi_context.models["sushi.fruits"].columns_to_types == {
        "id": exp.DataType.build("BIGINT"),
        "name": exp.DataType.build("VARCHAR"),
    }
