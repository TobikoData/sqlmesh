from __future__ import annotations

import typing as t

from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_model
from sqlmesh.core.test.definition import SqlModelTest
from sqlmesh.utils.yaml import load as load_yaml


def test_ctes(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_model(
                parse(
                    """
        MODEL (
            name sushi.foo,
            kind FULL,
        );

        WITH source AS (
            SELECT id FROM raw
        ),
        renamed AS (
            SELECT id as fid FROM source
        )
        SELECT fid FROM renamed;
        """
                )
            )
        ),
    )

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      rows:
        - id: 1
  outputs:
    ctes:
      source:
        rows:
        - id: 1
      renamed:
        rows:
        - fid: 1
    query:
      rows:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
    latest: 2022-01-01
            """
    )
    result = SqlModelTest(
        body=body["test_foo"],
        test_name="test_foo",
        model=model,
        models=sushi_context._models,
        engine_adapter=sushi_context._test_engine_adapter,
        path=None,
    ).run()
    assert result and result.wasSuccessful()
