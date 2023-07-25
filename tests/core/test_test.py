from __future__ import annotations

import typing as t

from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.test.definition import SqlModelTest
from sqlmesh.utils.yaml import load as load_yaml


def _run_test(body, test_name, model, sushi_context):
    return SqlModelTest(
        body=body[test_name],
        test_name=test_name,
        model=model,
        models=sushi_context._models,
        engine_adapter=sushi_context._test_engine_adapter,
        path=None,
    ).run()


def test_ctes(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
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
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
      renamed:
        - fid: 1
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_ctes_only(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
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
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
      renamed:
        - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_query_only(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
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
      - id: 1
  outputs:
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_with_rows(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
                parse(
                    """
        MODEL (
            name sushi.foo,
            kind FULL,
        );

        WITH source AS (
            SELECT id FROM raw
        )
        SELECT id FROM source;
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
    query:
      rows:
        - id: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_without_rows(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
                parse(
                    """
        MODEL (
            name sushi.foo,
            kind FULL,
        );

        WITH source AS (
            SELECT id FROM raw
        )
        SELECT id FROM source;
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
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
    query:
      - id: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_column_order(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
                parse(
                    """
        MODEL (
            name sushi.foo,
            kind FULL,
        );

        SELECT id, value, ds FROM raw;
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
      - id: 1
        value: 2
        ds: 3
  outputs:
    query:
      - id: 1
        ds: 3
        value: 2
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()


def test_nan(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
                parse(
                    """
        MODEL (
            name sushi.foo,
            kind FULL,
        );

        SELECT id, value, ds FROM raw;
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
      - id: 1
        value: nan
        ds: 3
  outputs:
    query:
      - id: 1
        value: null
        ds: 3
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _run_test(body, "test_foo", model, sushi_context)
    assert result and result.wasSuccessful()
