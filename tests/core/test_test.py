from __future__ import annotations

import datetime
import typing as t
from pathlib import Path
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core import constants as c
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    SparkConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.core.model import Model, SqlModel, load_sql_based_model, model
from sqlmesh.core.test.definition import ModelTest, PythonModelTest, SqlModelTest
from sqlmesh.utils.errors import ConfigError, TestError
from sqlmesh.utils.yaml import dump as dump_yaml
from sqlmesh.utils.yaml import load as load_yaml

if t.TYPE_CHECKING:
    from unittest import TestResult

pytestmark = pytest.mark.slow

SUSHI_FOO_META = "MODEL (name sushi.foo, kind FULL)"


def _create_test(
    body: t.Dict[str, t.Any], test_name: str, model: Model, context: Context
) -> ModelTest:
    test_type = SqlModelTest if isinstance(model, SqlModel) else PythonModelTest
    return test_type(
        body=body[test_name],
        test_name=test_name,
        model=model,
        models=context._models,
        engine_adapter=context._test_connection_config.create_engine_adapter(
            register_comments_override=False
        ),
        dialect=context.config.dialect,
        path=None,
        default_catalog=context.default_catalog,
    )


def _create_model(
    query: str,
    meta: str = SUSHI_FOO_META,
    dialect: t.Optional[str] = None,
    default_catalog: t.Optional[str] = None,
) -> SqlModel:
    parsed_definition = parse(f"{meta};{query}", default_dialect=dialect)
    return t.cast(
        SqlModel,
        load_sql_based_model(parsed_definition, dialect=dialect, default_catalog=default_catalog),
    )


def _check_successful_or_raise(
    result: t.Optional[TestResult], expected_msg: t.Optional[str] = None
) -> None:
    assert result is not None
    if not result.wasSuccessful():
        error_or_failure_traceback = (result.errors or result.failures)[0][1]
        if expected_msg:
            assert expected_msg in error_or_failure_traceback
        else:
            raise AssertionError(error_or_failure_traceback)


@pytest.fixture
def full_model_without_ctes(request) -> SqlModel:
    return _create_model(
        "SELECT id, value, ds FROM raw",
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


@pytest.fixture
def full_model_with_single_cte(request) -> SqlModel:
    return _create_model(
        "WITH source AS (SELECT id FROM raw) SELECT id FROM source",
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


@pytest.fixture
def full_model_with_two_ctes(request) -> SqlModel:
    return _create_model(
        """
        WITH source AS (
            SELECT id FROM raw
        ),
        renamed AS (
            SELECT id AS fid FROM source
        )
        SELECT fid FROM RENAMED;
        """,
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


def test_ctes(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_with_two_ctes),
            context=sushi_context,
        ).run()
    )


def test_ctes_only(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_with_two_ctes),
            context=sushi_context,
        ).run()
    )


def test_query_only(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_with_two_ctes),
            context=sushi_context,
        ).run()
    )


def test_with_rows(sushi_context: Context, full_model_with_single_cte: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_with_single_cte),
            context=sushi_context,
        ).run()
    )


def test_without_rows(sushi_context: Context, full_model_with_single_cte: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_with_single_cte),
            context=sushi_context,
        ).run()
    )


def test_column_order(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
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
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_without_ctes),
            context=sushi_context,
        ).run()
    )


def test_row_order(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    # Input and output rows are in different orders
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: 2
        ds: 3
      - id: 2
        value: 3
        ds: 4
  outputs:
    query:
      - id: 2
        value: 3
        ds: 4
      - id: 1
        value: 2
        ds: 3
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )

    # Model query without ORDER BY should pass unit test
    _check_successful_or_raise(
        _create_test(
            body=body,
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_without_ctes),
            context=sushi_context,
        ).run()
    )

    full_model_without_ctes_dict = full_model_without_ctes.dict()
    full_model_without_ctes_dict["query"] = full_model_without_ctes.query.order_by("id")  # type: ignore
    full_model_without_ctes_orderby = SqlModel(**full_model_without_ctes_dict)

    # Model query with ORDER BY should fail unit test
    _check_successful_or_raise(
        _create_test(
            body=body,
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_without_ctes_orderby),
            context=sushi_context,
        ).run(),
        expected_msg=(
            "AssertionError: Data mismatch (exp: expected, act: actual)\n\n"
            "   id     value      ds    \n"
            "  exp act   exp act exp act\n"
            "0   2   1     3   2   4   3\n"
            "1   1   2     2   3   3   4\n"
        ),
    )


@pytest.mark.parametrize(
    "waiter_names_input",
    [
        """sushi.waiter_names:
          - id: 1
          - id: 2
            name: null
          - id: 3
            name: 'bob'
        """,
        """sushi.waiter_names:
      format: csv
      rows: |
        id,name
        1,
        2,null
        3,bob""",
    ],
)
def test_partial_data(sushi_context: Context, waiter_names_input: str) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                f"""
test_foo:
  model: sushi.foo
  inputs:
    {waiter_names_input}
  outputs:
    ctes:
      source:
        - id: 1
        - id: 2
          name: null
        - id: 3
          name: 'bob'
    query:
      - id: 1
        str: nan
      - id: 2
        str: nan
      - id: 3
        name: 'bob'
        str: nan
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "WITH source AS (SELECT id, name FROM sushi.waiter_names) "
                    "SELECT id, name, 'nan' as str FROM source",
                    default_catalog=sushi_context.default_catalog,
                )
            ),
            context=sushi_context,
        ).run()
    )


@pytest.mark.parametrize(
    "waiter_names_input",
    [
        """sushi.waiter_names:
        format: yaml
        rows:
        - id: 1
          name: alice
        - id: 2
          name: 'bob'
        """,
        """sushi.waiter_names:
      format: csv
      rows: |
        id,name
        1,alice
        2,bob""",
        """sushi.waiter_names:
      format: csv
      csv_settings:
        sep: "#"
      rows: |
        id#name
        1#alice
        2#bob""",
    ],
)
def test_format_inline(sushi_context: Context, waiter_names_input: str) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                f"""
test_foo:
  model: sushi.foo
  inputs:
    {waiter_names_input}
  outputs:
    query:
      - id: 1
        name: alice
      - id: 2
        name: 'bob'
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT id, name FROM sushi.waiter_names ",
                    default_catalog=sushi_context.default_catalog,
                )
            ),
            context=sushi_context,
        ).run()
    )


@pytest.mark.parametrize(
    ["input_data", "filename", "file_data"],
    [
        [
            """sushi.waiter_names:
        format: yaml
        path: """,
            "test_data.yaml",
            """- id: 1
  name: alice
- id: 2
  name: 'bob'
""",
        ],
        [
            """sushi.waiter_names:
        path: """,
            "test_data.yaml",
            """rows:
- id: 1
  name: alice
- id: 2
  name: 'bob'
""",
        ],
        [
            """sushi.waiter_names:
        format: csv
        path: """,
            "test_data.csv",
            """id,name
1,alice
2,bob""",
        ],
    ],
)
def test_format_path(
    sushi_context: Context, tmp_path: Path, input_data: str, filename: str, file_data: str
) -> None:
    test_csv_file = tmp_path / filename
    test_csv_file.write_text(file_data)

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                f"""
test_foo:
  model: sushi.foo
  inputs:
    {input_data}{str(test_csv_file)}
  outputs:
    query:
      - id: 1
        name: alice
      - id: 2
        name: 'bob'
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT id, name FROM sushi.waiter_names ",
                    default_catalog=sushi_context.default_catalog,
                )
            ),
            context=sushi_context,
        ).run()
    )


def test_unsupported_format_failure(
    sushi_context: Context, full_model_without_ctes: SqlModel
) -> None:
    with pytest.raises(
        TestError,
        match="Unsupported data format 'xml' for 'sushi.waiter_names'",
    ):
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  description: XML format isn't supported to load data (fails intentionally)
  inputs:
    sushi.waiter_names:
      format: xml
      path: 'test_data.xml'
  outputs:
    query:
      - id: 1
        value: null
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_without_ctes),
            context=sushi_context,
        )

    with pytest.raises(
        TestError,
        match="Unsupported data format 'xml' for 'sushi.waiter_names'",
    ):
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  description: XML without path doesn't raise error
  inputs:
    sushi.waiter_names:
      format: xml
      rows: |
        <rows>
          <row>
              <id>1</id>
              <name>alice</name>
          </row>
          <row>
            <id>2</id>
            <name>bob</name>
          </row>
        </rows>
  outputs:
    query:
      - id: 1
        name: alice
      - id: 2
        name: 'bob'
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT id, name FROM sushi.waiter_names ",
                    default_catalog=sushi_context.default_catalog,
                )
            ),
            context=sushi_context,
        )


def test_partial_output_columns() -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - a: 1
        b: 2
        c: 3
        d: 4
      - a: 5
        b: 6
        c: 7
  outputs:
    partial: true  # Applies to all outputs
    ctes:
      t:
        rows:
          - c: 3
          - c: 7
    query:
      rows:
        - a: 1
          b: 2
        - a: 5
          b: 6
                """
            ),
            test_name="test_foo",
            model=_create_model("WITH t AS (SELECT a, b, c, d FROM raw) SELECT a, b, c, d FROM t"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - a: 1
        b: 2
        c: 3
        d: 4
      - a: 5
        b: 6
        c: 7
  outputs:
    ctes:
      t:
        partial: true
        rows:
          - c: 3
          - c: 7
    query:
      rows:
        - a: 1
          b: 2
          c: 3
          d: 4
        - a: 5
          b: 6
          c: 7
                """
            ),
            test_name="test_foo",
            model=_create_model("WITH t AS (SELECT a, b, c, d FROM raw) SELECT a, b, c, d FROM t"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_partial_data_column_order(sushi_context: Context) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    sushi.items:
      - id: 1234
        event_date: 2020-01-01
      - id: 9876
        name: hello
        event_date: 2020-01-02
  outputs:
    query:
      - id: 1234
        event_date: 2020-01-01
      - id: 9876
        name: hello
        event_date: 2020-01-02
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT id, name, price, event_date FROM sushi.items",
                    default_catalog=sushi_context.default_catalog,
                )
            ),
            context=sushi_context,
        ).run()
    )


def test_partial_data_missing_schemas(sushi_context: Context) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    unknown:
      - a: 1
        b: bla
      - b: baz
  outputs:
    query:
      - a: 1
        b: bla
      - b: baz
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(_create_model("SELECT * FROM unknown")),
            context=sushi_context,
        ).run()
    )
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    unknown:
      - id: 1234
        date: 2023-01-12
      - id: 9876
        date: 2023-02-10
  outputs:
    query:
      - id: 1234
        date: 2023-01-12
        month: 2023-01-01
        null_date:
      - id: 9876
        date: 2023-02-10
        month: 2023-02-01
        null_date:
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT *, DATE_TRUNC('month', date)::DATE AS month, NULL::DATE AS null_date, FROM unknown"
                )
            ),
            context=sushi_context,
        ).run()
    )


def test_partially_inferred_schemas(sushi_context: Context, mocker: MockerFixture) -> None:
    parent = _create_model(
        "SELECT a, b, s::ROW(d DATE) AS s FROM sushi.unknown",
        meta="MODEL (name sushi.parent, kind FULL, dialect trino)",
        default_catalog="memory",
    )
    parent = t.cast(SqlModel, sushi_context.upsert_model(parent))

    child = _create_model(
        "SELECT a, b, s.d AS d FROM sushi.parent",
        meta="MODEL (name sushi.child, kind FULL)",
        default_catalog="memory",
    )
    child = t.cast(SqlModel, sushi_context.upsert_model(child))

    body = load_yaml(
        """
test_child:
  model: sushi.child
  inputs:
    sushi.parent:
      - a: 1
        b: bla
        s:
          d: 2020-01-01
  outputs:
    query:
      - a: 1
        b: bla
        d: 2020-01-01
        """
    )

    mocker.patch("sqlmesh.core.test.definition.random_id", return_value="jzngz56a")
    test = _create_test(body, "test_child", child, sushi_context)

    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())

    spy_execute.assert_any_call(
        'CREATE OR REPLACE VIEW "memory"."sqlmesh_test_jzngz56a"."memory__sushi__parent" ("s", "a", "b") AS '
        "SELECT "
        'CAST("s" AS STRUCT("d" DATE)) AS "s", '
        'CAST("a" AS INT) AS "a", '
        'CAST("b" AS TEXT) AS "b" '
        """FROM (VALUES ({'d': CAST('2020-01-01' AS DATE)}, 1, 'bla')) AS "t"("s", "a", "b")"""
    )


def test_uninferrable_schema() -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - value: null
  outputs:
    query:
      - value: null
                """
            ),
            test_name="test_foo",
            model=_create_model("SELECT value FROM raw"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            """Failed to infer the data type of column 'value' for '"raw"'. This issue can """
            "be mitigated by casting the column in the model definition, setting its type in "
            "external_models.yaml if it's an external model, setting the model's 'columns' property, "
            "or setting its 'columns' mapping in the test itself\n"
        ),
    )


def test_missing_column_failure(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  description: sushi.foo's output has a missing column (fails intentionally)
  inputs:
    raw:
      - id: 1
        value: 2
        ds: 3
  outputs:
    query:
      - id: 1
        value: null
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(full_model_without_ctes),
            context=sushi_context,
        ).run(),
        expected_msg=(
            "AssertionError: Data mismatch (exp: expected, act: actual)\n\n"
            "  value        ds    \n"
            "    exp act   exp act\n"
            "0  None   2  None   3\n"
        ),
    )


def test_row_difference_failure() -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - value: 1
  outputs:
    query:
      - value: 1
      - value: 2
                """
            ),
            test_name="test_foo",
            model=_create_model("SELECT value FROM raw"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            "AssertionError: Data mismatch (rows are different)\n\n"
            "Missing rows:\n\n"
            "   value\n"
            "0      2\n"
        ),
    )
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - value: 1
  outputs:
    query:
      - value: 1
                """
            ),
            test_name="test_foo",
            model=_create_model(
                "SELECT value FROM raw UNION ALL SELECT value + 1 AS value FROM raw"
            ),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            "AssertionError: Data mismatch (rows are different)\n\n"
            "Unexpected rows:\n\n"
            "   value\n"
            "0      2\n"
        ),
    )
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - value: 1
  outputs:
    query:
      - value: 1
      - value: 3
      - value: 4
                """
            ),
            test_name="test_foo",
            model=_create_model(
                "SELECT value FROM raw UNION ALL SELECT value + 1 AS value FROM raw"
            ),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            "AssertionError: Data mismatch (rows are different)\n\n"
            "Missing rows:\n\n"
            "   value\n"
            "0      3\n"
            "1      4\n\n"
            "Unexpected rows:\n\n"
            "   value\n"
            "0      2\n"
        ),
    )


def test_unknown_column_error() -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: 2
  outputs:
    query:
      - foo: 1
                """
            ),
            test_name="test_foo",
            model=_create_model("SELECT id, value FROM raw"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            "sqlmesh.utils.errors.TestError: Detected unknown column(s)\n\n"
            "Expected column(s): id, value\n"
            "Unknown column(s): foo\n"
        ),
    )


def test_empty_rows(sushi_context: Context) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    sushi.items: []
  outputs:
    query: []
                """
            ),
            test_name="test_foo",
            model=sushi_context.upsert_model(
                _create_model(
                    "SELECT id FROM sushi.items", default_catalog=sushi_context.default_catalog
                )
            ),
            context=sushi_context,
        ).run()
    )

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_a:
  model: a
  inputs:
    b:
      columns:
        x: "array(int)"
      rows: []
  outputs:
    query: []
                """
            ),
            test_name="test_a",
            model=sushi_context.upsert_model(
                _create_model("SELECT x FROM b", default_catalog="memory")
            ),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


@pytest.mark.parametrize("full_model_without_ctes", ["snowflake"], indirect=True)
def test_normalization(full_model_without_ctes: SqlModel) -> None:
    normalized_body = _create_test(
        body=load_yaml(
            """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    ctes:
      source:
        rows:
          - id: 1
      renamed:
        - fid: 1
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
        ),
        test_name="test_foo",
        model=full_model_without_ctes,
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="snowflake"))),
    ).body

    expected_body = {
        "model": '"MEMORY"."SUSHI"."FOO"',
        "inputs": {'"RAW"': {"rows": [{"ID": 1}]}},
        "outputs": {
            "ctes": {'"SOURCE"': {"rows": [{"ID": 1}]}, '"RENAMED"': {"rows": [{"FID": 1}]}},
            "query": {"rows": [{"FID": 1}]},
        },
        "vars": {"start": datetime.date(2022, 1, 1), "end": datetime.date(2022, 1, 1)},
    }

    assert expected_body == normalized_body


def test_source_func() -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: xyz
  outputs:
    query:
      - month: 2023-01-01
      - month: 2023-02-01
      - month: 2023-03-01
                """
            ),
            test_name="test_foo",
            model=_create_model(
                """
                SELECT range::DATE AS month
                FROM RANGE(DATE '2023-01-01', DATE '2023-04-01', INTERVAL 1 MONTH) AS r
                """
            ),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_nested_data_types(sushi_context: Context) -> None:
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: sushi.foo
  inputs:
    sushi.raw:
      columns:
        array1: "INT[]"
        array2: "STRUCT(k VARCHAR, v STRUCT(v_str VARCHAR, v_int INT, v_int_arr INT[]))[]"
        struct: "STRUCT(x INT[], y VARCHAR, z INT, w STRUCT(a INT))"
      rows:
        - array1: [1, 2, 3]
          array2: [{'k': 'hello', 'v': {'v_str': 'there', 'v_int': 10, 'v_int_arr': [1, 2]}}]
          struct: {'x': [1, 2, 3], 'y': 'foo', 'z': 1, 'w': {'a': 5}}
        - array1:
          - 2
          - 3
        - array1: [0, 4, 1]
  outputs:
    query:
      - array1: [0, 4, 1]
      - array1: [1, 2, 3]
        array2: [{'k': 'hello', 'v': {'v_str': 'there', 'v_int': 10, 'v_int_arr': [1, 2]}}]
        struct: {'x': [1, 2, 3], 'y': 'foo', 'z': 1, 'w': {'a': 5}}
      - array1: [2, 3]
                """
            ),
            test_name="test_foo",
            model=_create_model(
                "SELECT array1, array2, struct FROM sushi.raw", default_catalog="memory"
            ),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_freeze_time(mocker: MockerFixture) -> None:
    mocker.patch("sqlmesh.core.test.definition.random_id", return_value="jzngz56a")
    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  outputs:
    query:
      - cur_date: 2023-01-01
        cur_time: 12:05:03
        cur_timestamp: "2023-01-01 12:05:03"
  vars:
    execution_time: "2023-01-01 12:05:03+00:00"
            """
        ),
        test_name="test_foo",
        model=_create_model(
            "SELECT CURRENT_DATE AS cur_date, CURRENT_TIME AS cur_time, CURRENT_TIMESTAMP AS cur_timestamp"
        ),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    )

    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())

    spy_execute.assert_has_calls(
        [
            call('CREATE SCHEMA IF NOT EXISTS "memory"."sqlmesh_test_jzngz56a"'),
            call(
                "SELECT "
                """CAST('2023-01-01 12:05:03+00:00' AS DATE) AS "cur_date", """
                """CAST('2023-01-01 12:05:03+00:00' AS TIME) AS "cur_time", """
                '''CAST('2023-01-01 12:05:03+00:00' AS TIMESTAMP) AS "cur_timestamp"'''
            ),
            call('DROP SCHEMA IF EXISTS "memory"."sqlmesh_test_jzngz56a" CASCADE'),
        ]
    )

    @model("py_model", columns={"ts1": "timestamptz", "ts2": "timestamptz"})
    def execute(context, start, end, execution_time, **kwargs):
        datetime_now = datetime.datetime.now()

        context.engine_adapter.execute(exp.select("CURRENT_TIMESTAMP"))
        current_timestamp = context.engine_adapter.cursor.fetchone()[0]

        return pd.DataFrame([{"ts1": datetime_now, "ts2": current_timestamp}])

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_py_model:
  model: py_model
  outputs:
    query:
      - ts1: "2023-01-01 10:05:03"
        ts2: "2023-01-01 10:05:03"
  vars:
    execution_time: "2023-01-01 12:05:03+02:00"
                """
            ),
            test_name="test_py_model",
            model=model.get_registry()["py_model"].model(module_path=Path("."), path=Path(".")),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_successes(sushi_context: Context) -> None:
    results = sushi_context.test()
    successful_tests = [success.test_name for success in results.successes]  # type: ignore
    assert len(successful_tests) == 3
    assert "test_order_items" in successful_tests
    assert "test_customer_revenue_by_day" in successful_tests


def test_create_external_model_fixture(sushi_context: Context, mocker: MockerFixture) -> None:
    mocker.patch("sqlmesh.core.test.definition.random_id", return_value="jzngz56a")
    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: sushi.foo
  inputs:
    c.db.external:
      - x: 1
  outputs:
    query:
      - x: 1
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT x FROM c.db.external"),
        context=sushi_context,
    )
    _check_successful_or_raise(test.run())

    assert len(test._fixture_table_cache) == 1
    for table in test._fixture_table_cache.values():
        assert table.catalog == "memory"
        assert table.db == "sqlmesh_test_jzngz56a"


def test_runtime_stage() -> None:
    @macro()
    def test_macro(evaluator: MacroEvaluator) -> t.List[bool]:
        return [evaluator.runtime_stage == "testing", evaluator.runtime_stage == "loading"]

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: foo
  outputs:
    query:
      - c: [true, false]
                """
            ),
            test_name="test_foo",
            model=_create_model("SELECT [@test_macro()] AS c"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_gateway(copy_to_temp_path: t.Callable, mocker: MockerFixture) -> None:
    path = Path(copy_to_temp_path("examples/sushi")[0])
    db_db_path = str(path / "db.db")
    test_db_path = str(path / "test.db")

    config = Config(
        gateways={
            "main": GatewayConfig(connection=DuckDBConnectionConfig(database=db_db_path)),
            "test": GatewayConfig(test_connection=DuckDBConnectionConfig(database=test_db_path)),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )

    context = Context(paths=path, config=config)

    test_path = path / c.TESTS / "test_customer_revenue_by_day.yaml"
    test_dict = load_yaml(test_path)
    test_dict["test_customer_revenue_by_day"]["gateway"] = "test"

    with open(test_path, "w", encoding="utf-8") as file:
        dump_yaml(test_dict, file)

    spy_execute = mocker.spy(EngineAdapter, "_execute")
    mocker.patch("sqlmesh.core.test.definition.random_id", return_value="jzngz56a")

    result = context.test(tests=[f"{test_path}::test_customer_revenue_by_day"])
    _check_successful_or_raise(result)

    expected_view_sql = (
        'CREATE OR REPLACE VIEW "test"."sqlmesh_test_jzngz56a"."db__sushi__orders" '
        '("id", "customer_id", "waiter_id", "start_ts", "end_ts", "event_date") AS '
        "SELECT "
        'CAST("id" AS INT) AS "id", '
        'CAST("customer_id" AS INT) AS "customer_id", '
        'CAST("waiter_id" AS INT) AS "waiter_id", '
        'CAST("start_ts" AS INT) AS "start_ts", '
        'CAST("end_ts" AS INT) AS "end_ts", '
        'CAST("event_date" AS DATE) AS "event_date" '
        "FROM (VALUES "
        "(1, 1, 1, 1641002340, 1641004140, CAST('2022-01-01' AS DATE)), "
        "(2, 1, 2, 1641007740, 1641009540, CAST('2022-01-01' AS DATE))) "
        'AS "t"("id", "customer_id", "waiter_id", "start_ts", "end_ts", "event_date")'
    )
    test_adapter = t.cast(ModelTest, result.successes[0]).engine_adapter
    assert call(test_adapter, expected_view_sql) in spy_execute.mock_calls

    _check_successful_or_raise(context.test())


def test_generate_input_data_using_sql(mocker: MockerFixture, tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")
    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    context = Context(paths=tmp_path, config=config)
    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_example_full_model_alt:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
      query: |
        SELECT 1 AS item_id, 1 AS id
        UNION ALL
        SELECT 1 AS item_id, 2 AS id
        UNION ALL
        SELECT 2 AS item_id, 3 AS id
        UNION ALL
        SELECT 3 AS item_id, 4 AS id
        UNION ALL
        SELECT 4 AS item_id, null AS id
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
      - item_id: 3
        num_orders: 1
      - item_id: 4
        num_orders: 0
                """
            ),
            test_name="test_example_full_model_alt",
            model=context.get_model("sqlmesh_example.full_model"),
            context=context,
        ).run()
    )

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_example_full_model_partial:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
      query: |
        SELECT 1 as id,
        UNION ALL
        SELECT 2 as id,
  outputs:
    query:
      partial: true
      rows:
      - item_id: null
        num_orders: 2
                """
            ),
            test_name="test_example_full_model_partial",
            model=context.get_model("sqlmesh_example.full_model"),
            context=context,
        ).run()
    )

    mocker.patch("sqlmesh.core.test.definition.random_id", return_value="jzngz56a")
    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  inputs:
    foo:
      query: "SELECT {'x': 1, 'n': {'y': 2}} AS struct_value"
  outputs:
    query:
      - struct_value: {'x': 1, 'n': {'y': 2}}
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT struct_value FROM foo"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    )
    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())

    spy_execute.assert_any_call(
        'CREATE OR REPLACE VIEW "memory"."sqlmesh_test_jzngz56a"."foo" AS '
        '''SELECT {'x': 1, 'n': {'y': 2}} AS "struct_value"'''
    )

    with pytest.raises(
        TestError,
        match="Invalid test, cannot set both 'query' and 'rows' for 'foo'",
    ):
        _create_test(
            body=load_yaml(
                """
test_foo:
  model: xyz
  inputs:
    foo:
      query: "SELECT {'x': 1, 'n': {'y': 2}} AS struct_value"
      rows:
        struct_value: {'x': 1, 'n': {'y': 2}}
  outputs:
    query:
      - struct_value: {'x': 1, 'n': {'y': 2}}
                """
            ),
            test_name="test_foo",
            model=_create_model("SELECT struct_value FROM foo"),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        )


def test_pyspark_python_model() -> None:
    spark_connection_config = SparkConnectionConfig(
        config={
            "spark.master": "local",
            "spark.sql.warehouse.dir": "/tmp/data_dir",
            "spark.driver.extraJavaOptions": "-Dderby.system.home=/tmp/derby_dir",
        },
    )
    config = Config(
        gateways=GatewayConfig(test_connection=spark_connection_config),
        model_defaults=ModelDefaultsConfig(dialect="spark"),
    )
    context = Context(config=config)

    @model("pyspark_model", columns={"col": "int"})
    def execute(context, start, end, execution_time, **kwargs):
        return context.spark.sql("SELECT 1 AS col")

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_pyspark_model:
  model: pyspark_model
  outputs:
    query:
      - col: 1
                """
            ),
            test_name="test_pyspark_model",
            model=model.get_registry()["pyspark_model"].model(
                module_path=Path("."), path=Path(".")
            ),
            context=context,
        ).run()
    )


def test_variable_usage(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        variables={"gold": "gold_db", "silver": "silver_db"},
    )
    context = Context(paths=tmp_path, config=config)

    parent = _create_model(
        "SELECT 1 AS id, '2022-01-02'::DATE AS ds, @start_ts AS start_ts",
        meta="MODEL (name silver_db.sch.b, kind INCREMENTAL_BY_TIME_RANGE(time_column ds))",
    )
    parent = t.cast(SqlModel, context.upsert_model(parent))

    child = _create_model(
        "SELECT ds, @IF(@VAR('myvar'), id, id + 1) AS id FROM silver_db.sch.b WHERE ds BETWEEN @start_ds and @end_ds",
        meta="MODEL (name gold_db.sch.a, kind INCREMENTAL_BY_TIME_RANGE(time_column ds))",
    )
    child = t.cast(SqlModel, context.upsert_model(child))

    test_file = tmp_path / "tests" / "test_parameterized_model_names.yaml"
    test_file.write_text(
        """
test_parameterized_model_names:
  model: {{ var('gold') }}.sch.a
  vars:
    myvar: True
    start_ds: 2022-01-01
    end_ds: 2022-01-03
  inputs:
    {{ var('silver') }}.sch.b:
      - ds: 2022-01-01
        id: 1
      - ds: 2022-01-01
        id: 2
  outputs:
    query:
      - ds: 2022-01-01
        id: 1
      - ds: 2022-01-01
        id: 2
        """
    )

    results = context.test()

    assert not results.failures
    assert not results.errors

    # The example project has one test and we added another one above
    assert len(results.successes) == 2


def test_test_generation(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    context = Context(paths=tmp_path, config=config)

    query = context.get_model("sqlmesh_example.full_model").render_query()
    assert isinstance(query, exp.Query)

    context.upsert_model(
        "sqlmesh_example.full_model",
        query=exp.select(*query.named_selects).from_("cte").with_("cte", as_=query),
    )

    context.plan(auto_apply=True)

    input_queries = {
        "sqlmesh_example.incremental_model": "SELECT * FROM sqlmesh_example.incremental_model LIMIT 3"
    }

    with pytest.raises(ConfigError) as ex:
        context.create_test("sqlmesh_example.full_model", input_queries=input_queries)

    assert (
        "tests/test_full_model.yaml' already exists, "
        "make sure to set --overwrite if it can be safely overwritten."
    ) in str(ex.value)

    test = load_yaml(context.path / c.TESTS / "test_full_model.yaml")

    assert len(test) == 1
    assert "test_example_full_model" in test
    assert "vars" not in test["test_example_full_model"]
    assert "ctes" not in test["test_example_full_model"]["outputs"]

    context.create_test(
        "sqlmesh_example.full_model",
        input_queries=input_queries,
        overwrite=True,
        variables={"start": "2020-01-01", "end": "2024-01-01"},
        include_ctes=True,
    )

    test = load_yaml(context.path / c.TESTS / "test_full_model.yaml")

    assert len(test) == 1
    assert "test_full_model" in test
    assert "vars" in test["test_full_model"]
    assert test["test_full_model"]["vars"] == {"start": "2020-01-01", "end": "2024-01-01"}
    assert "ctes" in test["test_full_model"]["outputs"]
    assert "cte" in test["test_full_model"]["outputs"]["ctes"]

    _check_successful_or_raise(context.test())

    context.create_test(
        "sqlmesh_example.full_model",
        input_queries=input_queries,
        name="new_name",
        path="foo/bar",
    )

    test = load_yaml(context.path / c.TESTS / "foo/bar.yaml")
    assert len(test) == 1
    assert "new_name" in test
    assert "ctes" not in test["new_name"]["outputs"]


@pytest.mark.parametrize(
    ["column", "expected"],
    [
        (  # Array of strings
            "['value1', 'value2']",
            [{"col": ["value1", "value2"]}],
        ),
        (  # Array of arrays
            "[['value1'], ['value2', 'value3']]",
            [{"col": [["value1"], ["value2", "value3"]]}],
        ),
        (  # Array of maps
            "[MAP {'key': 'value1'}, MAP {'key': 'value2'}]",
            [{"col": [{"key": "value1"}, {"key": "value2"}]}],
        ),
        (  # Array of structs
            "[{'key': 'value1'}, {'key': 'value2'}]",
            [{"col": [{"key": "value1"}, {"key": "value2"}]}],
        ),
        (  # Map of strings
            "MAP {'key1': 'value1', 'key2': 'value2'}",
            [{"col": {"key1": "value1", "key2": "value2"}}],
        ),
        (  # Map of arrays
            "MAP {'key1': ['value1'], 'key2': ['value2']}",
            [{"col": {"key1": ["value1"], "key2": ["value2"]}}],
        ),
        (  # Map of maps
            "MAP {'key1': MAP {'subkey1': 'value1'}, 'key2': MAP {'subkey2': 'value2'}}",
            [{"col": {"key1": {"subkey1": "value1"}, "key2": {"subkey2": "value2"}}}],
        ),
        (  # Map of structs
            "MAP {'key1': {'subkey': 'value1'}, 'key2': {'subkey': 'value2'}}",
            [{"col": {"key1": {"subkey": "value1"}, "key2": {"subkey": "value2"}}}],
        ),
        (  # Struct of strings
            "{'key1': 'value1', 'key2': 'value2'}",
            [{"col": {"key1": "value1", "key2": "value2"}}],
        ),
        (  # Struct of arrays
            "{'key1': ['value1'], 'key2': ['value2']}",
            [{"col": {"key1": ["value1"], "key2": ["value2"]}}],
        ),
        (  # Struct of maps
            "{'key1': MAP {'subkey1': 'value1'}, 'key2': MAP {'subkey2': 'value2'}}",
            [{"col": {"key1": {"subkey1": "value1"}, "key2": {"subkey2": "value2"}}}],
        ),
        (  # Struct of structs
            "{'key1': {'subkey1': 'value1'}, 'key2': {'subkey2': 'value2'}}",
            [{"col": {"key1": {"subkey1": "value1"}, "key2": {"subkey2": "value2"}}}],
        ),
    ],
)
def test_test_generation_with_data_structures(tmp_path: Path, column: str, expected: str) -> None:
    def create_test(context: Context, query: str):
        context.create_test(
            "sqlmesh_example.foo",
            input_queries={"sqlmesh_example.bar": query},
            overwrite=True,
        )
        return load_yaml(context.path / c.TESTS / "test_foo.yaml")

    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    foo_sql_file = tmp_path / "models" / "foo.sql"
    foo_sql_file.write_text(
        "MODEL (name sqlmesh_example.foo); SELECT col FROM sqlmesh_example.bar;"
    )
    bar_sql_file = tmp_path / "models" / "bar.sql"
    bar_sql_file.write_text("MODEL (name sqlmesh_example.bar); SELECT col FROM external_table;")

    test = create_test(Context(paths=tmp_path, config=config), f"SELECT {column} AS col")
    assert test["test_foo"]["inputs"] == {"sqlmesh_example.bar": expected}
    assert test["test_foo"]["outputs"] == {"query": expected}


def test_test_generation_with_timestamp(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    foo_sql_file = tmp_path / "models" / "foo.sql"
    foo_sql_file.write_text(
        "MODEL (name sqlmesh_example.foo); SELECT ts_col FROM sqlmesh_example.bar;"
    )
    bar_sql_file = tmp_path / "models" / "bar.sql"
    bar_sql_file.write_text("MODEL (name sqlmesh_example.bar); SELECT ts_col FROM external_table;")

    context = Context(paths=tmp_path, config=config)

    input_queries = {
        "sqlmesh_example.bar": "SELECT TIMESTAMP '2024-09-20 11:30:00.123456789' AS ts_col"
    }

    context.create_test(
        "sqlmesh_example.foo",
        input_queries=input_queries,
        overwrite=True,
    )

    test = load_yaml(context.path / c.TESTS / "test_foo.yaml")

    assert len(test) == 1
    assert "test_foo" in test
    assert test["test_foo"]["inputs"] == {
        "sqlmesh_example.bar": [{"ts_col": datetime.datetime(2024, 9, 20, 11, 30, 0, 123456)}]
    }
    assert test["test_foo"]["outputs"] == {
        "query": [{"ts_col": datetime.datetime(2024, 9, 20, 11, 30, 0, 123456)}]
    }
