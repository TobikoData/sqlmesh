from __future__ import annotations

import datetime
import typing as t
import io
from pathlib import Path
import unittest
from unittest.mock import call, patch
from shutil import copyfile

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp
from IPython.utils.capture import capture_output

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core import constants as c
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    SparkConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.context import Context, ExecutionContext
from sqlmesh.core.console import get_console
from sqlmesh.core.dialect import parse
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.core.model import Model, SqlModel, load_sql_based_model, model
from sqlmesh.core.test.definition import ModelTest, PythonModelTest, SqlModelTest
from sqlmesh.core.test.result import ModelTextTestResult
from sqlmesh.utils.errors import ConfigError, SQLMeshError, TestError
from sqlmesh.utils.yaml import dump as dump_yaml
from sqlmesh.utils.yaml import load as load_yaml

from tests.utils.test_helpers import use_terminal_console

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
        engine_adapter=context.test_connection_config.create_engine_adapter(
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

    model_sql = """
SELECT
    ARRAY_AGG(DISTINCT id_contact_b ORDER BY id_contact_b) AS aggregated_duplicates
FROM
    source
GROUP BY
    id_contact_a
ORDER BY
    id_contact_a
    """

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_array_order:
  model: test
  inputs:
    source:
    - id_contact_a: a
      id_contact_b: b
    - id_contact_a: a
      id_contact_b: c
  outputs:
    query:
    - aggregated_duplicates:
      - c
      - b
                """
            ),
            test_name="test_array_order",
            model=_create_model(model_sql),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run(),
        expected_msg=(
            """AssertionError: Data mismatch (exp: expected, act: actual)\n\n"""
            "  aggregated_duplicates        \n"
            "                    exp     act\n"
            "0                (c, b)  (b, c)\n"
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
            "sqlmesh.utils.errors.TestError: Failed to run test:\n"
            "Detected unknown column(s)\n\n"
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

    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  outputs:
    query:
      - cur_timestamp: "2023-01-01 12:05:03+00:00"
  vars:
    execution_time: "2023-01-01 12:05:03+00:00"
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT CURRENT_TIMESTAMP AS cur_timestamp"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="bigquery"))),
    )

    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())

    spy_execute.assert_has_calls(
        [call('''SELECT CAST('2023-01-01 12:05:03+00:00' AS TIMESTAMPTZ) AS "cur_timestamp"''')]
    )

    @model("py_model", columns={"ts1": "timestamptz", "ts2": "timestamptz"})
    def execute(context, start, end, execution_time, **kwargs):
        datetime_now_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        context.engine_adapter.execute(exp.select("CURRENT_TIMESTAMP"))
        current_timestamp = context.engine_adapter.cursor.fetchone()[0]

        return pd.DataFrame([{"ts1": datetime_now_utc, "ts2": current_timestamp}])

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

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_example_full_model_partial:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      partial: true
      query: "SELECT 2 AS num_orders UNION ALL SELECT 1 AS num_orders"
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


def test_pyspark_python_model(tmp_path: Path) -> None:
    spark_connection_config = SparkConnectionConfig(
        config={
            "spark.master": "local",
            "spark.sql.warehouse.dir": f"{tmp_path}/data_dir",
            "spark.driver.extraJavaOptions": f"-Dderby.system.home={tmp_path}/derby_dir",
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

    variables = {"gold": "gold_db", "silver": "silver_db"}
    incorrect_variables = {"gold": "foo", "silver": "bar"}

    parent = _create_model(
        "SELECT 1 AS id, '2022-01-02'::DATE AS ds, @start_ts AS start_ts",
        meta="MODEL (name silver_db.sch.b, kind INCREMENTAL_BY_TIME_RANGE(time_column ds))",
    )

    child = _create_model(
        "SELECT ds, @IF(@VAR('myvar'), id, id + 1) AS id FROM silver_db.sch.b WHERE ds BETWEEN @start_ds and @end_ds",
        meta="MODEL (name gold_db.sch.a, kind INCREMENTAL_BY_TIME_RANGE(time_column ds))",
    )

    test_text = """
test_parameterized_model_names:
  model: {{{{ var('gold') }}}}.sch.a {gateway}
  vars:
    myvar: True
    start_ds: 2022-01-01
    end_ds: 2022-01-03
  inputs:
    {{{{ var('silver') }}}}.sch.b:
      - ds: 2022-01-01
        id: 1
      - ds: 2022-01-01
        id: 2
  outputs:
    query:
      - ds: 2022-01-01
        id: 1
      - ds: 2022-01-01
        id: 2"""

    test_file = tmp_path / "tests" / "test_parameterized_model_names.yaml"

    def init_context_and_validate_results(config: Config, **kwargs):
        context = Context(paths=tmp_path, config=config, **kwargs)
        context.upsert_model(parent)
        context.upsert_model(child)

        results = context.test()

        assert not results.failures
        assert not results.errors
        assert len(results.successes) == 2

    # Case 1: Test root variables
    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        variables=variables,
    )

    test_file.write_text(test_text.format(gateway=""))

    init_context_and_validate_results(config)

    # Case 2: Test gateway variables
    config = Config(
        gateways={
            "main": GatewayConfig(connection=DuckDBConnectionConfig(), variables=variables),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    init_context_and_validate_results(config)

    # Case 3: Test gateway variables overriding root variables
    config = Config(
        gateways={
            "main": GatewayConfig(connection=DuckDBConnectionConfig(), variables=variables),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        variables=incorrect_variables,
    )
    init_context_and_validate_results(config, gateway="main")

    # Case 4: Use variable from the defined gateway
    config = Config(
        gateways={
            "main": GatewayConfig(
                connection=DuckDBConnectionConfig(), variables=incorrect_variables
            ),
            "secondary": GatewayConfig(connection=DuckDBConnectionConfig(), variables=variables),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )

    test_file.write_text(test_text.format(gateway="\n  gateway: secondary"))
    init_context_and_validate_results(config, gateway="main")

    # Case 5: Use gateways with escaped characters
    config = Config(
        gateways={
            "main": GatewayConfig(
                connection=DuckDBConnectionConfig(), variables=incorrect_variables
            ),
            "secon\tdary": GatewayConfig(connection=DuckDBConnectionConfig(), variables=variables),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )

    test_file.write_text(test_text.format(gateway='\n  gateway: "secon\\tdary"'))
    init_context_and_validate_results(config, gateway="main")


def test_custom_testing_schema(mocker: MockerFixture) -> None:
    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  schema: my_schema
  outputs:
    query:
      - a: 1
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT 1 AS a"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    )

    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())

    spy_execute.assert_has_calls(
        [
            call('CREATE SCHEMA IF NOT EXISTS "memory"."my_schema"'),
            call('SELECT 1 AS "a"'),
            call('DROP SCHEMA IF EXISTS "memory"."my_schema" CASCADE'),
        ]
    )


def test_pretty_query(mocker: MockerFixture) -> None:
    test = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  schema: my_schema
  outputs:
    query:
      - a: 1
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT 1 AS a"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    )
    test.engine_adapter._pretty_sql = True
    spy_execute = mocker.spy(test.engine_adapter, "_execute")
    _check_successful_or_raise(test.run())
    spy_execute.assert_has_calls(
        [
            call('CREATE SCHEMA IF NOT EXISTS "memory"."my_schema"'),
            call('SELECT\n  1 AS "a"'),
            call('DROP SCHEMA IF EXISTS "memory"."my_schema" CASCADE'),
        ]
    )


def test_complicated_recursive_cte() -> None:
    model_sql = """
WITH
    RECURSIVE
    chained_contacts AS (
        -- Start with the initial set of contacts and their immediate nodes
        SELECT
            id_contact_a,
            id_contact_b
        FROM
            source

        UNION ALL

        -- Recursive step to find further connected nodes
        SELECT
            chained_contacts.id_contact_a,
            unfactorized_duplicates.id_contact_b
        FROM
            chained_contacts
                JOIN source AS unfactorized_duplicates
                     ON chained_contacts.id_contact_b = unfactorized_duplicates.id_contact_a
    ),
    id_contact_a_with_aggregated_id_contact_bs AS (
        SELECT
            id_contact_a,
            ARRAY_AGG(DISTINCT id_contact_b ORDER BY id_contact_b) AS aggregated_id_contact_bs
        FROM
            chained_contacts
        GROUP BY
            id_contact_a
    )
SELECT
    ARRAY_CONCAT([id_contact_a], aggregated_id_contact_bs) AS aggregated_duplicates
FROM
    id_contact_a_with_aggregated_id_contact_bs
WHERE
    id_contact_a NOT IN (
        SELECT DISTINCT
            id_contact_b
        FROM
            source
    )
ORDER BY
    id_contact_a
    """

    _check_successful_or_raise(
        _create_test(
            body=load_yaml(
                """
test_recursive_ctes:
  model: test
  inputs:
    source:
      rows:
        - id_contact_a: "a"
          id_contact_b: "b"
        - id_contact_a: "b"
          id_contact_b: "c"
        - id_contact_a: "c"
          id_contact_b: "d"
        - id_contact_a: "a"
          id_contact_b: "g"
        - id_contact_a: "b"
          id_contact_b: "e"
        - id_contact_a: "c"
          id_contact_b: "f"
        - id_contact_a: "x"
          id_contact_b: "y"
  outputs:
    ctes:
      id_contact_a_with_aggregated_id_contact_bs:
        - id_contact_a: a
          aggregated_id_contact_bs: [b, c, d, e, f, g]
        - id_contact_a: x
          aggregated_id_contact_bs: [y]
        - id_contact_a: b
          aggregated_id_contact_bs: [c, d, e, f]
        - id_contact_a: c
          aggregated_id_contact_bs: [d, f]
    query:
      rows:
        - aggregated_duplicates: [a, b, c, d, e, f, g]
        - aggregated_duplicates: [x, y]
                """
            ),
            test_name="test_recursive_ctes",
            model=_create_model(model_sql),
            context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
        ).run()
    )


def test_unknown_model_warns(mocker: MockerFixture) -> None:
    body = load_yaml(
        """
model: unknown
outputs:
  query:
  - c: 1
        """
    )

    with patch.object(get_console(), "log_warning") as mock_logger:
        ModelTest.create_test(
            body=body,
            test_name="test_unknown_model",
            models={},  # type: ignore
            engine_adapter=mocker.Mock(),
            dialect=None,
            path=None,
        )
        assert mock_logger.mock_calls == [call("Model '\"unknown\"' was not found")]


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
    assert test["test_foo"]["inputs"] == {'"memory"."sqlmesh_example"."bar"': expected}
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
    context.create_test("sqlmesh_example.foo", input_queries=input_queries, overwrite=True)

    test = load_yaml(context.path / c.TESTS / "test_foo.yaml")

    assert len(test) == 1
    assert "test_foo" in test
    assert test["test_foo"]["inputs"] == {
        '"memory"."sqlmesh_example"."bar"': [
            {"ts_col": datetime.datetime(2024, 9, 20, 11, 30, 0, 123456)}
        ]
    }
    assert test["test_foo"]["outputs"] == {
        "query": [{"ts_col": datetime.datetime(2024, 9, 20, 11, 30, 0, 123456)}]
    }


def test_test_generation_with_decimal(tmp_path: Path, mocker: MockerFixture) -> None:
    from decimal import Decimal

    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    foo_sql_file = tmp_path / "models" / "foo.sql"
    foo_sql_file.write_text(
        "MODEL (name sqlmesh_example.foo); SELECT dec_col FROM sqlmesh_example.bar;"
    )
    bar_sql_file = tmp_path / "models" / "bar.sql"
    bar_sql_file.write_text("MODEL (name sqlmesh_example.bar); SELECT dec_col FROM external_table;")

    context = Context(paths=tmp_path, config=config)
    input_queries = {
        '"memory"."sqlmesh_example"."bar"': "SELECT CAST(1.23 AS DECIMAL(10,2)) AS dec_col"
    }

    # DuckDB actually returns a numpy.float64, even though the value is cast into a DECIMAL,
    # but other engines don't behave the same. E.g. BigQuery returns a proper Decimal value.
    mocker.patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.fetchdf",
        return_value=pd.DataFrame({"dec_col": [Decimal("1.23")]}),
    )

    context.create_test("sqlmesh_example.foo", input_queries=input_queries, overwrite=True)

    test = load_yaml(context.path / c.TESTS / "test_foo.yaml")

    assert len(test) == 1
    assert "test_foo" in test
    assert test["test_foo"]["inputs"] == {'"memory"."sqlmesh_example"."bar"': [{"dec_col": "1.23"}]}
    assert test["test_foo"]["outputs"] == {"query": [{"dec_col": "1.23"}]}


def test_test_generation_with_recursive_ctes(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    foo_sql_file = tmp_path / "models" / "foo.sql"
    foo_sql_file.write_text(
        "MODEL (name sqlmesh_example.foo);"
        "WITH RECURSIVE t AS (SELECT 1 AS c UNION ALL SELECT c + 1 FROM t WHERE c < 3) SELECT c FROM t"
    )

    context = Context(paths=tmp_path, config=config)
    context.plan(auto_apply=True)

    context.create_test("sqlmesh_example.foo", input_queries={}, overwrite=True, include_ctes=True)

    test = load_yaml(context.path / c.TESTS / "test_foo.yaml")
    assert len(test) == 1
    assert "test_foo" in test
    assert test["test_foo"]["inputs"] == {}
    assert test["test_foo"]["outputs"] == {
        "query": [{"c": 1}, {"c": 2}, {"c": 3}],
        "ctes": {
            "t": [{"c": 1}, {"c": 2}, {"c": 3}],
        },
    }

    _check_successful_or_raise(context.test())


def test_test_with_gateway_specific_model(tmp_path: Path, mocker: MockerFixture) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        gateways={
            "main": GatewayConfig(connection=DuckDBConnectionConfig()),
            "second": GatewayConfig(connection=DuckDBConnectionConfig()),
        },
        default_gateway="main",
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    gw_model_sql_file = tmp_path / "models" / "gw_model.sql"

    # The model has a gateway specified which isn't the default
    gw_model_sql_file.write_text(
        "MODEL (name sqlmesh_example.gw_model, gateway second); SELECT c FROM sqlmesh_example.input_model;"
    )
    input_model_sql_file = tmp_path / "models" / "input_model.sql"
    input_model_sql_file.write_text(
        "MODEL (name sqlmesh_example.input_model); SELECT c FROM external_table;"
    )

    context = Context(paths=tmp_path, config=config)
    input_queries = {'"memory"."sqlmesh_example"."input_model"': "SELECT 5 AS c"}
    mocker.patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.fetchdf",
        return_value=pd.DataFrame({"c": [5]}),
    )

    assert context.engine_adapter == context.engine_adapters["main"]
    with pytest.raises(
        SQLMeshError, match=r"Gateway 'wrong' not found in the available engine adapters."
    ):
        context._get_engine_adapter("wrong")

    # Create test should use the gateway specific engine adapter
    context.create_test("sqlmesh_example.gw_model", input_queries=input_queries, overwrite=True)
    assert context._get_engine_adapter("second") == context.engine_adapters["second"]
    assert len(context.engine_adapters) == 2

    test = load_yaml(context.path / c.TESTS / "test_gw_model.yaml")

    assert len(test) == 1
    assert "test_gw_model" in test
    assert test["test_gw_model"]["inputs"] == {
        '"memory"."sqlmesh_example"."input_model"': [{"c": 5}]
    }
    assert test["test_gw_model"]["outputs"] == {"query": [{"c": 5}]}


def test_test_with_resolve_template_macro(tmp_path: Path):
    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "foo.sql").write_text(
        """
      MODEL (
        name test.foo,
        kind full,
        physical_properties (
          location = @resolve_template('file:///tmp/@{table_name}')
        )
      );

      SELECT t.a + 1 as a
      FROM @resolve_template('@{schema_name}.dev_@{table_name}', mode := 'table') as t
      """
    )

    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_foo.yaml").write_text(
        """
test_resolve_template_macro:
  model: test.foo
  inputs:
    test.dev_foo:
      - a: 1
  outputs:
    query:
      - a: 2
    """
    )

    context = Context(paths=tmp_path, config=config)
    _check_successful_or_raise(context.test())


def test_test_output(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    original_test_file = tmp_path / "tests" / "test_full_model.yaml"

    new_test_file = tmp_path / "tests" / "test_full_model_error.yaml"
    new_test_file.write_text(
        """
test_example_full_model:
  model: sqlmesh_example.full_model
  description: This is a test
  inputs:
    sqlmesh_example.incremental_model:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 2 
        """
    )

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        default_test_connection=DuckDBConnectionConfig(concurrent_tasks=8),
    )
    context = Context(paths=tmp_path, config=config)

    # Case 1: Assert the log report is structured correctly
    with capture_output() as output:
        context.test()

    # Order may change due to concurrent execution
    assert "F." in output.stderr or ".F" in output.stderr
    assert (
        f"""======================================================================
FAIL: test_example_full_model ({new_test_file})
This is a test
----------------------------------------------------------------------
AssertionError: Data mismatch (exp: expected, act: actual)

  num_orders     
         exp  act
1        2.0  1.0

----------------------------------------------------------------------"""
        in output.stderr
    )

    assert "Ran 2 tests" in output.stderr
    assert "FAILED (failures=1)" in output.stderr

    # Case 2: Assert that concurrent execution is working properly
    for i in range(50):
        copyfile(original_test_file, tmp_path / "tests" / f"test_success_{i}.yaml")
        copyfile(new_test_file, tmp_path / "tests" / f"test_failure_{i}.yaml")

    with capture_output() as output:
        context.test()

    assert "Ran 102 tests" in output.stderr
    assert "FAILED (failures=51)" in output.stderr


@use_terminal_console
def test_test_output_with_invalid_model_name(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    wrong_test_file = tmp_path / "tests" / "test_incorrect_model_name.yaml"
    wrong_test_file.write_text(
        """
test_example_full_model:
  model: invalid_model
  description: This is an invalid test
  inputs:
    sqlmesh_example.incremental_model:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 2 
        """
    )

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    context = Context(paths=tmp_path, config=config)

    with patch.object(get_console(), "log_warning") as mock_logger:
        with capture_output() as output:
            context.test()

        assert (
            f"""Model '"invalid_model"' was not found at {wrong_test_file}"""
            in mock_logger.call_args[0][0]
        )
        assert (
            ".\n----------------------------------------------------------------------\nRan 1 test in"
            in output.stderr
        )
        assert "OK" in output.stderr


def test_number_of_tests_found(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    # Example project contains 1 test and we add a new file with 2 tests
    test_file = tmp_path / "tests" / "test_new.yaml"
    test_file.write_text(
        """
test_example_full_model1:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
        
test_example_full_model2:
  model: sqlmesh_example.full_model
  inputs:
    sqlmesh_example.incremental_model:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
        """
    )

    context = Context(paths=tmp_path)

    # Case 1: All 3 tests should run without any tests specified
    results = context.test()
    assert len(results.successes) == 3

    # Case 2: The "new_test.yaml" should amount to 2 subtests
    results = context.test(tests=[f"{test_file}"])
    assert len(results.successes) == 2

    # Case 3: The "new_test.yaml::test_example_full_model2" should amount to a single subtest
    results = context.test(tests=[f"{test_file}::test_example_full_model2"])
    assert len(results.successes) == 1


def test_freeze_time_concurrent(tmp_path: Path) -> None:
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()

    macros_dir = tmp_path / "macros"
    macros_dir.mkdir()

    macro_file = macros_dir / "test_datetime_now.py"
    macro_file.write_text(
        """
from sqlglot import exp
import datetime
from sqlmesh.core.macros import macro

@macro()
def test_datetime_now(evaluator):
  return exp.cast(exp.Literal.string(datetime.datetime.now(tz=datetime.timezone.utc)), exp.DataType.Type.DATE)
  
@macro()
def test_sqlglot_expr(evaluator):
  return exp.CurrentDate().sql(evaluator.dialect)
    """
    )

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_model1 = models_dir / "sql_model1.sql"
    sql_model1.write_text(
        """
        MODEL(NAME sql_model1);
        SELECT @test_datetime_now() AS col_exec_ds_time, @test_sqlglot_expr() AS col_current_date;
        """
    )

    for model_name in ["sql_model1", "sql_model2", "py_model"]:
        for i in range(5):
            test_2019 = tmp_path / "tests" / f"test_2019_{model_name}_{i}.yaml"
            test_2019.write_text(
                f"""
    test_2019_{model_name}_{i}:
      model: {model_name}
      vars:
        execution_time: '2019-12-01'
      outputs:
        query:
          rows:
            - col_exec_ds_time: '2019-12-01'
              col_current_date: '2019-12-01'
              """
            )

            test_2025 = tmp_path / "tests" / f"test_2025_{model_name}_{i}.yaml"
            test_2025.write_text(
                f"""
    test_2025_{model_name}_{i}:
      model: {model_name}
      vars:
        execution_time: '2025-12-01'
      outputs:
        query:
          rows:
            - col_exec_ds_time: '2025-12-01'
              col_current_date: '2025-12-01'
              """
            )

    ctx = Context(
        paths=tmp_path,
        config=Config(default_test_connection=DuckDBConnectionConfig(concurrent_tasks=8)),
    )

    @model(
        "py_model",
        columns={"col_exec_ds_time": "timestamp_ntz", "col_current_date": "timestamp_ntz"},
    )
    def execute(context, start, end, execution_time, **kwargs):
        datetime_now_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        context.engine_adapter.execute(exp.select("CURRENT_DATE()"))
        current_date = context.engine_adapter.cursor.fetchone()[0]

        return pd.DataFrame(
            [{"col_exec_ds_time": datetime_now_utc, "col_current_date": current_date}]
        )

    python_model = model.get_registry()["py_model"].model(module_path=Path("."), path=Path("."))
    ctx.upsert_model(python_model)

    ctx.upsert_model(
        _create_model(
            meta="MODEL(NAME sql_model2)",
            query="SELECT @execution_ds::timestamp_ntz AS col_exec_ds_time, current_date()::date AS col_current_date",
            default_catalog=ctx.default_catalog,
        )
    )

    results = ctx.test()
    assert len(results.successes) == 30


def test_python_model_upstream_table(sushi_context) -> None:
    @model(
        "test_upstream_table_python",
        columns={"customer_id": "int", "zip": "str"},
    )
    def upstream_table_python(context, **kwargs):
        demographics_external_table = context.resolve_table("memory.raw.demographics")
        return context.fetchdf(
            exp.select("customer_id", "zip").from_(demographics_external_table),
        )

    python_model = model.get_registry()["test_upstream_table_python"].model(
        module_path=Path("."),
        path=Path("."),
    )

    context = ExecutionContext(sushi_context.engine_adapter, sushi_context.snapshots, None, None)
    df = list(python_model.render(context=context))[0]

    # Verify the actual model output matches the expected actual external table's values
    assert df.to_dict(orient="records") == [{"customer_id": 1, "zip": "00000"}]

    # Use different input values for the test and verify the outputs
    _check_successful_or_raise(
        _create_test(
            body=load_yaml("""
test_test_upstream_table_python:
  model: test_upstream_table_python
  inputs:
    memory.raw.demographics:
      - customer_id: 12
        zip: "S11HA"
      - customer_id: 555
        zip: "94401"
  outputs:
    query:
      - customer_id: 12
        zip: "S11HA"
      - customer_id: 555
        zip: "94401"
"""),
            test_name="test_test_upstream_table_python",
            model=model.get_registry()["test_upstream_table_python"].model(
                module_path=Path("."), path=Path(".")
            ),
            context=sushi_context,
        ).run()
    )


@pytest.mark.parametrize("is_error", [True, False])
def test_model_test_text_result_reporting_no_traceback(
    sushi_context: Context, full_model_with_two_ctes: SqlModel, is_error: bool
) -> None:
    test = _create_test(
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
    )
    stream = io.StringIO()
    result = ModelTextTestResult(
        stream=unittest.runner._WritelnDecorator(stream),  # type: ignore
        verbosity=1,
        descriptions=True,
    )

    try:
        raise Exception("failure")
    except Exception as e:
        assert e.__traceback__ is not None
        if is_error:
            result.addError(test, (e.__class__, e, e.__traceback__))
        else:
            result.addFailure(test, (e.__class__, e, e.__traceback__))

    result.log_test_report(0)

    stream.seek(0)
    output = stream.read()

    # Make sure that the traceback is not printed
    assert "Traceback" not in output
    assert "File" not in output
    assert "line" not in output

    prefix = "ERROR" if is_error else "FAIL"
    assert f"{prefix}: test_foo (None)" in output
    assert "Exception: failure" in output
