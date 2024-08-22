import agate
from datetime import datetime
import json
import logging
import typing as t
from pathlib import Path
from unittest.mock import patch

import pytest
from dbt.adapters.base import BaseRelation
from dbt.exceptions import CompilationError
from freezegun import freeze_time
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one
from sqlmesh.core import dialect as d
from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.context import Context
from sqlmesh.core.model import (
    EmbeddedKind,
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    IncrementalUnmanagedKind,
    ManagedKind,
    SqlModel,
    ViewKind,
)
from sqlmesh.core.model.kind import SCDType2ByColumnKind, SCDType2ByTimeKind
from sqlmesh.core.state_sync.engine_adapter import _snapshot_to_json
from sqlmesh.dbt.builtin import _relation_info_to_relation
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
)
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import Materialization, ModelConfig
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.relation import Policy
from sqlmesh.dbt.seed import SeedConfig, Integer
from sqlmesh.dbt.target import BigQueryConfig, DuckDbConfig, SnowflakeConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils.errors import ConfigError, MacroEvalError, SQLMeshError

pytestmark = [pytest.mark.dbt, pytest.mark.slow]


def test_model_name():
    context = DbtContext()
    context._target = DuckDbConfig(name="duckdb", schema="foo")
    assert ModelConfig(schema="foo", path="models/bar.sql").canonical_name(context) == "foo.bar"
    assert (
        ModelConfig(schema="foo", path="models/bar.sql", alias="baz").canonical_name(context)
        == "foo.baz"
    )
    assert (
        ModelConfig(
            database="memory", schema="foo", path="models/bar.sql", alias="baz"
        ).canonical_name(context)
        == "foo.baz"
        == "foo.baz"
    )
    assert (
        ModelConfig(
            database="other", schema="foo", path="models/bar.sql", alias="baz"
        ).canonical_name(context)
        == "other.foo.baz"
    )


def test_model_kind():
    context = DbtContext()
    context.project_name = "Test"
    context.target = DuckDbConfig(name="target", schema="foo")

    assert ModelConfig(materialized=Materialization.TABLE).model_kind(context) == FullKind()
    assert ModelConfig(materialized=Materialization.VIEW).model_kind(context) == ViewKind()
    assert ModelConfig(materialized=Materialization.EPHEMERAL).model_kind(context) == EmbeddedKind()
    assert ModelConfig(
        materialized=Materialization.SNAPSHOT,
        unique_key=["id"],
        updated_at="updated_at",
        strategy="timestamp",
    ).model_kind(context) == SCDType2ByTimeKind(
        unique_key=["id"],
        valid_from_name="dbt_valid_from",
        valid_to_name="dbt_valid_to",
        updated_at_as_valid_from=True,
        updated_at_name="updated_at",
        dialect="duckdb",
    )
    assert ModelConfig(
        materialized=Materialization.SNAPSHOT,
        unique_key=["id"],
        strategy="check",
        check_cols=["foo"],
    ).model_kind(context) == SCDType2ByColumnKind(
        unique_key=["id"],
        valid_from_name="dbt_valid_from",
        valid_to_name="dbt_valid_to",
        columns=["foo"],
        execution_time_as_valid_from=True,
        dialect="duckdb",
    )
    assert ModelConfig(
        materialized=Materialization.SNAPSHOT,
        unique_key=["id"],
        strategy="check",
        check_cols=["foo"],
        dialect="bigquery",
    ).model_kind(context) == SCDType2ByColumnKind(
        unique_key=["id"],
        valid_from_name="dbt_valid_from",
        valid_to_name="dbt_valid_to",
        columns=["foo"],
        execution_time_as_valid_from=True,
        dialect="bigquery",
    )

    assert ModelConfig(materialized=Materialization.INCREMENTAL, time_column="foo").model_kind(
        context
    ) == IncrementalByTimeRangeKind(time_column="foo", dialect="duckdb", forward_only=True)
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="delete+insert",
        forward_only=False,
    ).model_kind(context) == IncrementalByTimeRangeKind(time_column="foo", dialect="duckdb")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="insert_overwrite",
    ).model_kind(context) == IncrementalByTimeRangeKind(
        time_column="foo", dialect="duckdb", forward_only=True
    )
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        unique_key=["bar"],
        dialect="bigquery",
    ).model_kind(context) == IncrementalByTimeRangeKind(
        time_column="foo", dialect="bigquery", forward_only=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], incremental_strategy="merge"
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=False
    )

    assert ModelConfig(materialized=Materialization.INCREMENTAL, unique_key=["bar"]).model_kind(
        context
    ) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=False
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], full_refresh=False
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], full_refresh=True
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=False
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"], disable_restatement=True
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        unique_key=["bar"],
        disable_restatement=True,
        full_refresh=True,
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        unique_key=["bar"],
        disable_restatement=True,
        full_refresh=False,
    ).model_kind(context) == IncrementalByUniqueKeyKind(
        unique_key=["bar"], dialect="duckdb", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", incremental_strategy="merge"
    ).model_kind(context) == IncrementalByTimeRangeKind(
        time_column="foo", dialect="duckdb", forward_only=True, disable_restatement=False
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="append",
        disable_restatement=True,
    ).model_kind(context) == IncrementalByTimeRangeKind(
        time_column="foo", dialect="duckdb", forward_only=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        time_column="foo",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar"},
        forward_only=False,
    ).model_kind(context) == IncrementalByTimeRangeKind(time_column="foo", dialect="duckdb")

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar"},
    ).model_kind(context) == IncrementalUnmanagedKind(
        insert_overwrite=True, disable_restatement=False
    )

    assert ModelConfig(materialized=Materialization.INCREMENTAL).model_kind(
        context
    ) == IncrementalUnmanagedKind(insert_overwrite=True, disable_restatement=False)

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, incremental_strategy="append"
    ).model_kind(context) == IncrementalUnmanagedKind(disable_restatement=False)

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, incremental_strategy="append", full_refresh=None
    ).model_kind(context) == IncrementalUnmanagedKind(disable_restatement=False)

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar", "data_type": "int64"},
    ).model_kind(context) == IncrementalUnmanagedKind(
        insert_overwrite=True, disable_restatement=False
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar", "data_type": "int64"},
        full_refresh=False,
    ).model_kind(context) == IncrementalUnmanagedKind(
        insert_overwrite=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar", "data_type": "int64"},
        disable_restatement=True,
        full_refresh=True,
    ).model_kind(context) == IncrementalUnmanagedKind(
        insert_overwrite=True, disable_restatement=True
    )

    assert ModelConfig(
        materialized=Materialization.INCREMENTAL,
        incremental_strategy="insert_overwrite",
        partition_by={"field": "bar", "data_type": "int64"},
        disable_restatement=True,
    ).model_kind(context) == IncrementalUnmanagedKind(
        insert_overwrite=True, disable_restatement=True
    )

    assert (
        ModelConfig(materialized=Materialization.DYNAMIC_TABLE, target_lag="1 hour").model_kind(
            context
        )
        == ManagedKind()
    )

    with pytest.raises(ConfigError):
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="delete+insert",
        ).model_kind(context)
    with pytest.raises(ConfigError):
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="insert_overwrite",
        ).model_kind(context)
    with pytest.raises(ConfigError):
        ModelConfig(
            materialized=Materialization.INCREMENTAL,
            unique_key=["bar"],
            incremental_strategy="append",
        ).model_kind(context)


def test_model_kind_snapshot_bigquery():
    context = DbtContext()
    context.project_name = "Test"
    context.target = BigQueryConfig(name="target", schema="foo", project="bar")

    assert ModelConfig(
        materialized=Materialization.SNAPSHOT,
        unique_key=["id"],
        updated_at="updated_at",
        strategy="timestamp",
    ).model_kind(context) == SCDType2ByTimeKind(
        unique_key=["id"],
        valid_from_name="dbt_valid_from",
        valid_to_name="dbt_valid_to",
        updated_at_as_valid_from=True,
        updated_at_name="updated_at",
        time_data_type=exp.DataType.build("TIMESTAMPTZ"),
        dialect="bigquery",
    )

    # time_data_type is bigquery version even though model dialect is DuckDB
    # because model target is BigQuery
    assert ModelConfig(
        materialized=Materialization.SNAPSHOT,
        unique_key=["id"],
        updated_at="updated_at",
        strategy="timestamp",
        dialect="duckdb",
    ).model_kind(context) == SCDType2ByTimeKind(
        unique_key=["id"],
        valid_from_name="dbt_valid_from",
        valid_to_name="dbt_valid_to",
        updated_at_as_valid_from=True,
        updated_at_name="updated_at",
        time_data_type=exp.DataType.build("TIMESTAMPTZ"),  # bigquery version
        dialect="duckdb",
    )


def test_model_columns():
    model = ModelConfig(
        alias="test",
        target_schema="foo",
        table_name="bar",
        sql="SELECT * FROM baz",
        columns={
            "ADDRESS": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "ZIPCODE": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
            "DATE": ColumnConfig(
                name="date", data_type="timestamp_ntz", description="Contract date"
            ),
        },
    )

    expected_column_types = {
        "ADDRESS": parse_one("text", into=exp.DataType),
        "ZIPCODE": parse_one("varchar(5)", into=exp.DataType),
        "DATE": parse_one("timestamp_ntz", into=exp.DataType, dialect="snowflake"),
    }
    expected_column_descriptions = {
        "ADDRESS": "Business address",
        "ZIPCODE": "Business zipcode",
        "DATE": "Contract date",
    }

    assert column_types_to_sqlmesh(model.columns, "snowflake") == expected_column_types
    assert column_descriptions_to_sqlmesh(model.columns) == expected_column_descriptions

    context = DbtContext()
    context.project_name = "Foo"
    context.target = SnowflakeConfig(
        name="target", schema="test", database="test", account="foo", user="bar", password="baz"
    )
    sqlmesh_model = model.to_sqlmesh(context)
    assert sqlmesh_model.columns_to_types == expected_column_types
    assert sqlmesh_model.column_descriptions == expected_column_descriptions


def test_seed_columns():
    seed = SeedConfig(
        name="foo",
        package="package",
        path=Path("examples/sushi_dbt/seeds/waiter_names.csv"),
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode", data_type="text", description="Business zipcode"
            ),
        },
    )

    expected_column_types = {
        "address": parse_one("text", into=exp.DataType),
        "zipcode": parse_one("text", into=exp.DataType),
    }
    expected_column_descriptions = {
        "address": "Business address",
        "zipcode": "Business zipcode",
    }

    context = DbtContext()
    context.project_name = "Foo"
    context.target = DuckDbConfig(name="target", schema="test")
    sqlmesh_seed = seed.to_sqlmesh(context)
    assert sqlmesh_seed.columns_to_types == expected_column_types
    assert sqlmesh_seed.column_descriptions == expected_column_descriptions


def test_seed_column_inference(tmp_path):
    seed_csv = tmp_path / "seed.csv"
    with open(seed_csv, "w", encoding="utf-8") as fd:
        fd.write("int_col,double_col,datetime_col,date_col,boolean_col,text_col\n")
        fd.write("1,1.2,2021-01-01 00:00:00,2021-01-01,true,foo\n")
        fd.write("2,2.3,2021-01-02 00:00:00,2021-01-02,false,bar\n")
        fd.write("null,,null,,,null\n")

    seed = SeedConfig(
        name="test_model",
        package="package",
        path=Path(seed_csv),
    )

    context = DbtContext()
    context.project_name = "Foo"
    context.target = DuckDbConfig(name="target", schema="test")
    sqlmesh_seed = seed.to_sqlmesh(context)
    assert sqlmesh_seed.columns_to_types == {
        "int_col": exp.DataType.build("int"),
        "double_col": exp.DataType.build("double"),
        "datetime_col": exp.DataType.build("datetime"),
        "date_col": exp.DataType.build("date"),
        "boolean_col": exp.DataType.build("boolean"),
        "text_col": exp.DataType.build("text"),
    }


def test_agate_integer_cast():
    agate_integer = Integer(null_values=("null", ""))
    assert agate_integer.cast("1") == 1
    assert agate_integer.cast(1) == 1
    assert agate_integer.cast("null") is None
    assert agate_integer.cast("") is None

    with pytest.raises(agate.exceptions.CastError):
        agate_integer.cast("1.2")

    with pytest.raises(agate.exceptions.CastError):
        agate_integer.cast(1.2)

    with pytest.raises(agate.exceptions.CastError):
        agate_integer.cast(datetime.now())


@pytest.mark.xdist_group("dbt_manifest")
def test_model_dialect(sushi_test_project: Project, assert_exp_eq):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        alias="table",
        sql="SELECT 1 AS `one` FROM {{ schema }}",
    )
    context = sushi_test_project.context

    # cannot parse model sql without specifying bigquery dialect
    with pytest.raises(ConfigError):
        model_config.to_sqlmesh(context).render_query_or_raise().sql()

    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        alias="table",
        sql="SELECT 1 AS `one` FROM {{ schema }}",
        dialect="bigquery",
    )
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "sushi" AS "sushi"',
    )


@pytest.mark.xdist_group("dbt_manifest")
@pytest.mark.parametrize(
    "model_fqn", ['"memory"."sushi"."waiters"', '"memory"."sushi"."waiter_names"']
)
def test_hooks(sushi_test_dbt_context: Context, model_fqn: str):
    engine_adapter = sushi_test_dbt_context.engine_adapter
    waiters = sushi_test_dbt_context.models[model_fqn]

    logger = logging.getLogger("sqlmesh.dbt.builtin")
    with patch.object(logger, "debug") as mock_logger:
        engine_adapter.execute(
            waiters.render_pre_statements(
                engine_adapter=engine_adapter, execution_time="2023-01-01"
            )
        )
    assert "pre-hook" in mock_logger.call_args[0][0]

    with patch.object(logger, "debug") as mock_logger:
        engine_adapter.execute(
            waiters.render_post_statements(
                engine_adapter=sushi_test_dbt_context.engine_adapter, execution_time="2023-01-01"
            )
        )
    assert "post-hook" in mock_logger.call_args[0][0]


@pytest.mark.xdist_group("dbt_manifest")
def test_target_jinja(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ target.name }}") == "in_memory"
    assert context.render("{{ target.schema }}") == "sushi"
    assert context.render("{{ target.type }}") == "duckdb"
    # Path and Profile name are not included in serializable fields
    assert context.render("{{ target.path }}") == "None"
    assert context.render("{{ target.profile_name }}") == "None"


@pytest.mark.xdist_group("dbt_manifest")
def test_project_name_jinja(sushi_test_project: Project):
    context = sushi_test_project.context
    assert context.render("{{ project_name }}") == "sushi"


@pytest.mark.xdist_group("dbt_manifest")
def test_schema_jinja(sushi_test_project: Project, assert_exp_eq):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        alias="table",
        sql="SELECT 1 AS one FROM {{ schema }}",
    )
    context = sushi_test_project.context
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "sushi" AS "sushi"',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_config_jinja(sushi_test_project: Project):
    hook = "{{ config(alias='bar') }} {{ config.alias }}"
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        sql="""SELECT 1 AS one FROM foo""",
        alias="model",
        **{"pre-hook": hook},
    )
    context = sushi_test_project.context
    model = t.cast(SqlModel, model_config.to_sqlmesh(context))
    assert hook in model.pre_statements[0].sql()
    assert model.render_pre_statements()[0].sql() == '"bar"'


@pytest.mark.xdist_group("dbt_manifest")
def test_model_this(assert_exp_eq, sushi_test_project: Project):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="schema",
        alias="test",
        sql="SELECT 1 AS one FROM {{ this.identifier }}",
    )
    context = sushi_test_project.context
    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_test_this(assert_exp_eq, sushi_test_project: Project):
    test_config = TestConfig(
        name="test",
        alias="alias",
        database="database",
        schema_="schema",
        standalone=True,
        sql="SELECT 1 AS one FROM {{ this.identifier }}",
    )
    context = sushi_test_project.context
    audit = t.cast(StandaloneAudit, test_config.to_sqlmesh(context))
    assert_exp_eq(
        audit.render_query(audit).sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_test_dialect(assert_exp_eq, sushi_test_project: Project):
    test_config = TestConfig(
        name="test",
        alias="alias",
        database="database",
        schema_="schema",
        standalone=True,
        sql="SELECT 1 AS `one` FROM {{ this.identifier }}",
    )
    context = sushi_test_project.context

    # can't parse test sql without specifying bigquery as default dialect
    with pytest.raises(ConfigError):
        audit = t.cast(StandaloneAudit, test_config.to_sqlmesh(context))
        audit.render_query(audit).sql()

    test_config.dialect_ = "bigquery"
    audit = t.cast(StandaloneAudit, test_config.to_sqlmesh(context))
    assert_exp_eq(
        audit.render_query(audit).sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_statement(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)
    assert (
        renderer(
            "{% set test_var = 'SELECT 1' %}{% call statement('something', fetch_result=True) %} {{ test_var }} {% endcall %}{{ load_result('something').table }}",
        )
        == """| column | data_type |
| ------ | --------- |
| 1      | Integer   |
"""
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_run_query(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)
    assert (
        renderer(
            """{% set results = run_query('SELECT 1 UNION ALL SELECT 2') %}{% for val in results.columns[0] %}{{ val }} {% endfor %}"""
        )
        == "1 2 "
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_logging(sushi_test_project: Project, runtime_renderer: t.Callable):
    context = sushi_test_project.context
    assert context.target
    engine_adapter = context.target.to_sqlmesh().create_engine_adapter()
    renderer = runtime_renderer(context, engine_adapter=engine_adapter)

    logger = logging.getLogger("sqlmesh.dbt.builtin")
    with patch.object(logger, "debug") as mock_logger:
        assert renderer('{{ log("foo") }}') == ""
    assert "foo" in mock_logger.call_args[0][0]

    with patch.object(logger, "debug") as mock_logger:
        assert renderer('{{ print("bar") }}') == ""
    assert "bar" in mock_logger.call_args[0][0]


@pytest.mark.xdist_group("dbt_manifest")
def test_exceptions(sushi_test_project: Project):
    context = sushi_test_project.context

    logger = logging.getLogger("sqlmesh.dbt.builtin")
    with patch.object(logger, "warning") as mock_logger:
        assert context.render('{{ exceptions.warn("Warning") }}') == ""
    assert "Warning" in mock_logger.call_args[0][0]

    with pytest.raises(CompilationError, match="Error"):
        context.render('{{ exceptions.raise_compiler_error("Error") }}')


@pytest.mark.xdist_group("dbt_manifest")
def test_modules(sushi_test_project: Project):
    context = sushi_test_project.context

    # datetime
    assert context.render("{{ modules.datetime.date(2022, 12, 25) }}") == "2022-12-25"

    # pytz
    try:
        assert "UTC" in context.render("{{ modules.pytz.all_timezones }}")
    except AttributeError as error:
        assert "object has no attribute 'pytz'" in str(error)

    # re
    assert context.render("{{ modules.re.search('(?<=abc)def', 'abcdef').group(0) }}") == "def"

    # itertools
    itertools_jinja = (
        "{% for num in modules.itertools.accumulate([5]) %}" "{{ num }}" "{% endfor %}"
    )
    assert context.render(itertools_jinja) == "5"


@pytest.mark.xdist_group("dbt_manifest")
def test_flags(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ flags.FULL_REFRESH }}") == "None"
    assert context.render("{{ flags.STORE_FAILURES }}") == "None"
    assert context.render("{{ flags.WHICH }}") == "parse"


@pytest.mark.xdist_group("dbt_manifest")
def test_relation(sushi_test_project: Project):
    context = sushi_test_project.context

    assert (
        context.render("{{ api.Relation }}")
        == "<class 'dbt.adapters.duckdb.relation.DuckDBRelation'>"
    )

    jinja = (
        "{% set relation = api.Relation.create(schema='sushi', identifier='waiters') %}"
        "{{ relation.schema }} {{ relation.identifier}}"
    )

    assert context.render(jinja) == "sushi waiters"


@pytest.mark.xdist_group("dbt_manifest")
def test_column(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ api.Column }}") == "<class 'dbt.adapters.base.column.Column'>"

    jinja = (
        "{% set col = api.Column('foo', 'integer') %}" "{{ col.is_integer() }} {{ col.is_string()}}"
    )

    assert context.render(jinja) == "True False"


@pytest.mark.xdist_group("dbt_manifest")
def test_quote(sushi_test_project: Project):
    context = sushi_test_project.context

    jinja = "{{ adapter.quote('foo') }} {{ adapter.quote('bar') }}"
    assert context.render(jinja) == '"foo" "bar"'


@pytest.mark.xdist_group("dbt_manifest")
def test_as_filters(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ True | as_bool }}") == "True"
    with pytest.raises(MacroEvalError, match="Failed to convert 'invalid' into boolean."):
        context.render("{{ 'invalid' | as_bool }}")

    assert context.render("{{ 123 | as_number }}") == "123"
    with pytest.raises(MacroEvalError, match="Failed to convert 'invalid' into number."):
        context.render("{{ 'invalid' | as_number }}")

    assert context.render("{{ None | as_text }}") == ""

    assert context.render("{{ None | as_native }}") == "None"


@pytest.mark.xdist_group("dbt_manifest")
def test_set(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ set([1, 1, 2]) }}") == "{1, 2}"
    assert context.render("{{ set(1) }}") == "None"

    assert context.render("{{ set_strict([1, 1, 2]) }}") == "{1, 2}"
    with pytest.raises(TypeError):
        assert context.render("{{ set_strict(1) }}")


@pytest.mark.xdist_group("dbt_manifest")
def test_json(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ tojson({'key': 'value'}) }}") == """{"key": "value"}"""
    assert context.render("{{ tojson(set([1])) }}") == "None"

    assert context.render("""{{ fromjson('{"key": "value"}') }}""") == "{'key': 'value'}"
    assert context.render("""{{ fromjson('invalid') }}""") == "None"


@pytest.mark.xdist_group("dbt_manifest")
def test_yaml(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ toyaml({'key': 'value'}) }}").strip() == "key: value"
    assert context.render("{{ toyaml(invalid) }}", invalid=lambda: "") == "None"

    assert context.render("""{{ fromyaml('key: value') }}""") == "{'key': 'value'}"


@pytest.mark.xdist_group("dbt_manifest")
def test_zip(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ zip([1, 2], ['a', 'b']) }}") == "[(1, 'a'), (2, 'b')]"
    assert context.render("{{ zip(12, ['a', 'b']) }}") == "None"

    assert context.render("{{ zip_strict([1, 2], ['a', 'b']) }}") == "[(1, 'a'), (2, 'b')]"
    with pytest.raises(TypeError):
        context.render("{{ zip_strict(12, ['a', 'b']) }}")


@pytest.mark.xdist_group("dbt_manifest")
def test_dbt_version(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ dbt_version }}").startswith("1.")


@pytest.mark.xdist_group("dbt_manifest")
def test_parsetime_adapter_call(
    assert_exp_eq, sushi_test_project: Project, sushi_test_dbt_context: Context
):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        alias="test",
        schema="sushi",
        sql="""
            {% set results = run_query('select 1 as one') %}
            SELECT {{ results.columns[0].values()[0] }} AS one FROM {{ this.identifier }}
        """,
    )
    context = sushi_test_project.context

    sqlmesh_model = model_config.to_sqlmesh(context)
    assert sqlmesh_model.render_query() is None
    assert sqlmesh_model.columns_to_types is None
    assert not sqlmesh_model.annotated
    with pytest.raises(SQLMeshError):
        sqlmesh_model.ctas_query()

    engine_adapter = sushi_test_dbt_context.engine_adapter
    assert_exp_eq(
        sqlmesh_model.render_query_or_raise(engine_adapter=engine_adapter).sql(),
        'SELECT 1 AS "one" FROM "test" AS "test"',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_partition_by(sushi_test_project: Project):
    context = sushi_test_project.context
    context.target = BigQueryConfig(name="production", database="main", schema="sushi")
    model_config = ModelConfig(
        name="model",
        alias="model",
        schema="test",
        package_name="package",
        materialized="table",
        unique_key="ds",
        partition_by={"field": "ds", "granularity": "month"},
        sql="""SELECT 1 AS one, ds FROM foo""",
    )
    date_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert date_trunc_expr.sql(dialect="bigquery") == "DATE_TRUNC(`ds`, MONTH)"
    assert date_trunc_expr.sql() == "DATE_TRUNC('MONTH', \"ds\")"

    model_config.partition_by = {"field": "`ds`", "data_type": "datetime", "granularity": "day"}
    datetime_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert datetime_trunc_expr.sql(dialect="bigquery") == "datetime_trunc(`ds`, DAY)"
    assert datetime_trunc_expr.sql() == 'DATETIME_TRUNC("ds", DAY)'

    model_config.partition_by = {"field": "ds", "data_type": "timestamp", "granularity": "day"}
    timestamp_trunc_expr = model_config.to_sqlmesh(context).partitioned_by[0]
    assert timestamp_trunc_expr.sql(dialect="bigquery") == "timestamp_trunc(`ds`, DAY)"
    assert timestamp_trunc_expr.sql() == 'TIMESTAMP_TRUNC("ds", DAY)'

    model_config.partition_by = {
        "field": "one",
        "data_type": "int64",
        "range": {"start": 0, "end": 10, "interval": 2},
    }
    assert (
        model_config.to_sqlmesh(context).partitioned_by[0].sql()
        == 'RANGE_BUCKET("one", GENERATE_SERIES(0, 10, 2))'
    )

    model_config.partition_by = {"field": "ds", "data_type": "date", "granularity": "day"}
    assert model_config.to_sqlmesh(context).partitioned_by == [exp.to_column("ds", quoted=True)]


@pytest.mark.xdist_group("dbt_manifest")
def test_relation_info_to_relation():
    assert _relation_info_to_relation(
        {"quote_policy": {}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=True, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": None, "schema": None, "identifier": None}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=True, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False, "schema": None, "identifier": None}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=True, identifier=True)

    assert _relation_info_to_relation(
        {"quote_policy": {"database": False, "schema": False, "identifier": False}},
        BaseRelation,
        Policy(database=True, schema=True, identifier=True),
    ).quote_policy == Policy(database=False, schema=False, identifier=False)


@pytest.mark.xdist_group("dbt_manifest")
def test_is_incremental(sushi_test_project: Project, assert_exp_eq, mocker):
    model_config = ModelConfig(
        name="model",
        package_name="package",
        schema="sushi",
        alias="some_table",
        sql="""
        SELECT 1 AS one FROM tbl_a
        {% if is_incremental() %}
        WHERE ds > (SELECT MAX(ds) FROM model)
        {% endif %}
        """,
    )
    context = sushi_test_project.context

    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise().sql(),
        'SELECT 1 AS "one" FROM "tbl_a" AS "tbl_a"',
    )

    snapshot = mocker.Mock()
    snapshot.intervals = [1]

    assert_exp_eq(
        model_config.to_sqlmesh(context).render_query_or_raise(snapshot=snapshot).sql(),
        'SELECT 1 AS "one" FROM "tbl_a" AS "tbl_a" WHERE "ds" > (SELECT MAX("ds") FROM "model" AS "model")',
    )


@pytest.mark.xdist_group("dbt_manifest")
def test_dbt_max_partition(sushi_test_project: Project, assert_exp_eq, mocker: MockerFixture):
    model_config = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        schema="sushi",
        partition_by={"field": "`ds`", "data_type": "datetime", "granularity": "month"},
        materialized=Materialization.INCREMENTAL,
        sql="""
        SELECT 1 AS one FROM tbl_a
        {% if is_incremental() %}
        WHERE ds > _dbt_max_partition
        {% endif %}
        """,
    )
    context = sushi_test_project.context
    context.target = BigQueryConfig(
        name="test_target", schema="test_schema", database="test-project"
    )

    pre_statement = model_config.to_sqlmesh(context).pre_statements[-1]  # type: ignore

    assert (
        pre_statement.sql().strip()
        == """
JINJA_STATEMENT_BEGIN;
{% if is_incremental() %}
  DECLARE _dbt_max_partition DATETIME DEFAULT (
    COALESCE((SELECT MAX(PARSE_DATETIME('%Y%m', partition_id)) FROM `{{ target.database }}`.`{{ adapter.resolve_schema(this) }}`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name = '{{ adapter.resolve_identifier(this) }}' AND NOT partition_id IS NULL AND partition_id <> '__NULL__'), CAST('1970-01-01' AS DATETIME))
  );
{% endif %}
JINJA_END;""".strip()
    )

    assert d.parse_one(pre_statement.sql()) == pre_statement


@pytest.mark.xdist_group("dbt_manifest")
def test_bigquery_physical_properties(sushi_test_project: Project, mocker: MockerFixture):
    context = sushi_test_project.context
    context.target = BigQueryConfig(
        name="test_target", schema="test_schema", database="test-project"
    )

    base_config = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        schema="sushi",
        partition_by={"field": "`ds`", "data_type": "datetime", "granularity": "month"},
        materialized=Materialization.INCREMENTAL,
        sql="SELECT 1 AS one FROM tbl_a",
    )

    assert base_config.to_sqlmesh(context).physical_properties == {}

    assert base_config.copy(
        update={"require_partition_filter": True, "partition_expiration_days": 7}
    ).to_sqlmesh(context).physical_properties == {
        "require_partition_filter": exp.convert(True),
        "partition_expiration_days": exp.convert(7),
    }

    assert base_config.copy(update={"require_partition_filter": True}).to_sqlmesh(
        context
    ).physical_properties == {
        "require_partition_filter": exp.convert(True),
    }

    assert base_config.copy(update={"partition_expiration_days": 7}).to_sqlmesh(
        context
    ).physical_properties == {
        "partition_expiration_days": exp.convert(7),
    }


@pytest.mark.xdist_group("dbt_manifest")
def test_snapshot_json_payload():
    sushi_context = Context(paths=["tests/fixtures/dbt/sushi_test"])
    snapshot_json = json.loads(
        _snapshot_to_json(sushi_context.get_snapshot("sushi.top_waiters", raise_if_missing=True))
    )
    assert snapshot_json["node"]["jinja_macros"]["global_objs"]["target"] == {
        "type": "duckdb",
        "name": "in_memory",
        "schema": "sushi",
        "database": "memory",
        "target_name": "in_memory",
    }


@pytest.mark.xdist_group("dbt_manifest")
@freeze_time("2023-01-08 00:00:00")
def test_dbt_package_macros(sushi_test_project: Project):
    context = sushi_test_project.context

    # Make sure external macros are available.
    assert context.render("{{ dbt.current_timestamp() }}") == "now()"
    # Make sure builtins are available too.
    assert context.render("{{ dbt.run_started_at }}") == "2023-01-08 00:00:00+00:00"


@pytest.mark.xdist_group("dbt_manifest")
def test_dbt_vars(sushi_test_project: Project):
    context = sushi_test_project.context

    assert context.render("{{ var('some_other_var') }}") == "5"
    assert context.render("{{ var('some_other_var', 0) }}") == "5"
    assert context.render("{{ var('missing') }}") == "None"
    assert context.render("{{ var('missing', 0) }}") == "0"

    assert context.render("{{ var.has_var('some_other_var') }}") == "True"
    assert context.render("{{ var.has_var('missing') }}") == "False"


@pytest.mark.xdist_group("dbt_manifest")
def test_snowflake_session_properties(sushi_test_project: Project, mocker: MockerFixture):
    context = sushi_test_project.context
    context.target = SnowflakeConfig(
        name="target", schema="test", database="test", account="foo", user="bar", password="baz"
    )

    base_config = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        schema="sushi",
        partition_by={"field": "`ds`", "data_type": "datetime", "granularity": "month"},
        materialized=Materialization.INCREMENTAL,
        sql="SELECT 1 AS one FROM tbl_a",
    )

    assert base_config.to_sqlmesh(context).session_properties == {}

    model_with_warehouse = base_config.copy(
        update={"snowflake_warehouse": "test_warehouse"}
    ).to_sqlmesh(context)

    assert model_with_warehouse.session_properties_ == exp.Tuple(
        expressions=[exp.Literal.string("warehouse").eq(exp.Literal.string("test_warehouse"))]
    )
    assert model_with_warehouse.session_properties == {"warehouse": "test_warehouse"}


def test_model_cluster_by():
    context = DbtContext()
    context._target = SnowflakeConfig(
        name="target",
        schema="test",
        database="test",
        account="account",
        user="user",
        password="password",
    )

    model = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        target_schema="test",
        cluster_by="Bar",
        sql="SELECT * FROM baz",
        materialized=Materialization.TABLE.value,
    )
    assert model.to_sqlmesh(context).clustered_by == ["BAR"]

    model = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        target_schema="test",
        cluster_by=["Bar", "qux"],
        sql="SELECT * FROM baz",
        materialized=Materialization.TABLE.value,
    )
    assert model.to_sqlmesh(context).clustered_by == ["BAR", "QUX"]


def test_snowflake_dynamic_table():
    context = DbtContext()
    context._target = SnowflakeConfig(
        name="target",
        schema="test",
        database="test",
        account="account",
        user="user",
        password="password",
    )

    model = ModelConfig(
        name="model",
        alias="model",
        package_name="package",
        target_schema="test",
        sql="SELECT * FROM baz",
        materialized=Materialization.DYNAMIC_TABLE.value,
        target_lag="1 hour",
        snowflake_warehouse="SMALL",
    )

    as_sqlmesh = model.to_sqlmesh(context)
    assert as_sqlmesh.kind == ManagedKind()
    assert as_sqlmesh.physical_properties == {
        "target_lag": exp.Literal.string("1 hour"),
        "warehouse": exp.Literal.string("SMALL"),
    }

    # both target_lag and snowflake_warehouse are required properties
    # https://docs.getdbt.com/reference/resource-configs/snowflake-configs#dynamic-tables
    for required_property in ["target_lag", "snowflake_warehouse"]:
        with pytest.raises(ConfigError, match=r".*must be set for dynamic tables"):
            model.copy(update={required_property: None}).to_sqlmesh(context)


@pytest.mark.xdist_group("dbt_manifest")
def test_refs_in_jinja_globals(sushi_test_project: Project, mocker: MockerFixture):
    context = sushi_test_project.context

    sqlmesh_model = t.cast(
        SqlModel,
        sushi_test_project.packages["sushi"].models["simple_model_b"].to_sqlmesh(context),
    )
    assert set(sqlmesh_model.jinja_macros.global_objs["refs"].keys()) == {"simple_model_a"}  # type: ignore

    sqlmesh_model = t.cast(
        SqlModel,
        sushi_test_project.packages["sushi"].models["top_waiters"].to_sqlmesh(context),
    )
    assert set(sqlmesh_model.jinja_macros.global_objs["refs"].keys()) == {  # type: ignore
        "waiter_revenue_by_day",
        "sushi.waiter_revenue_by_day",
    }
