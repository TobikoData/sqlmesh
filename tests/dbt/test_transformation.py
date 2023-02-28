from pathlib import Path

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    ModelKind,
    ModelKindName,
)
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
)
from sqlmesh.dbt.common import DbtContext
from sqlmesh.dbt.model import Materialization, ModelConfig
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.utils.errors import ConfigError


def test_model_name():
    assert ModelConfig(target_schema="foo", table_name="bar").model_name == "foo.bar"
    assert ModelConfig(target_schema="foo", table_name="bar", alias="baz").model_name == "foo.baz"
    assert (
        ModelConfig(target_schema="foo", table_name="bar", schema_="baz").model_name
        == "foo_baz.bar"
    )


def test_model_kind():
    assert ModelConfig(materialized=Materialization.TABLE).model_kind == ModelKind(
        name=ModelKindName.FULL
    )
    assert ModelConfig(materialized=Materialization.VIEW).model_kind == ModelKind(
        name=ModelKindName.VIEW
    )
    assert ModelConfig(materialized=Materialization.EPHEMERAL).model_kind == ModelKind(
        name=ModelKindName.EMBEDDED
    )
    with pytest.raises(ConfigError) as exception:
        ModelConfig(materialized=Materialization.INCREMENTAL).model_kind
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo"
    ).model_kind == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, time_column="foo", unique_key=["bar"]
    ).model_kind == IncrementalByTimeRangeKind(time_column="foo")
    assert ModelConfig(
        materialized=Materialization.INCREMENTAL, unique_key=["bar"]
    ).model_kind == IncrementalByUniqueKeyKind(unique_key=["bar"])


def test_model_columns():
    model = ModelConfig(
        target_schema="foo",
        table_name="bar",
        sql="SELECT * FROM baz",
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
        },
    )

    expected_column_types = {
        "address": parse_one("text", into=exp.DataType),
        "zipcode": parse_one("varchar(5)", into=exp.DataType),
    }
    expected_column_descriptions = {
        "address": "Business address",
        "zipcode": "Business zipcode",
    }

    assert column_types_to_sqlmesh(model.columns) == expected_column_types
    assert column_descriptions_to_sqlmesh(model.columns) == expected_column_descriptions

    context = DbtContext()
    sqlmesh_model = model.to_sqlmesh(context, {}, {})
    assert sqlmesh_model.columns_to_types == expected_column_types
    assert sqlmesh_model.column_descriptions == expected_column_descriptions


def test_seed_columns():
    seed = SeedConfig(
        seed_name="foo",
        path=Path("examples/sushi_dbt/seeds/waiter_names.csv"),
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode", data_type="varchar(5)", description="Business zipcode"
            ),
        },
    )

    expected_column_types = {
        "address": parse_one("text", into=exp.DataType),
        "zipcode": parse_one("varchar(5)", into=exp.DataType),
    }
    expected_column_descriptions = {
        "address": "Business address",
        "zipcode": "Business zipcode",
    }

    sqlmesh_seed = seed.to_sqlmesh()
    assert sqlmesh_seed.columns_to_types == expected_column_types
    assert sqlmesh_seed.column_descriptions == expected_column_descriptions


def test_config_containing_jinja():
    model = ModelConfig(
        **{"pre-hook": "GRANT INSERT ON {{ source('package', 'table') }}"},
        target_schema="{{ var('schema') }}",
        table_name="bar",
        sql="SELECT * FROM {{ source('package', 'table') }}",
        columns={
            "address": ColumnConfig(
                name="address", data_type="text", description="Business address"
            ),
            "zipcode": ColumnConfig(
                name="zipcode",
                data_type="varchar({{ var('size') }})",
                description="Business zipcode",
            ),
        },
    )

    context = DbtContext()
    context.variables = {"schema": "foo", "size": "5"}
    model._dependencies.sources = set(["package.table"])
    context.sources = {"package.table": "raw.baz"}

    rendered = model.render_config(context)
    assert rendered.pre_hook == model.pre_hook
    assert rendered.sql == model.sql
    assert rendered.target_schema != model.target_schema
    assert rendered.target_schema == "foo"
    assert rendered.columns["zipcode"] != model.columns["zipcode"]
    assert rendered.columns["zipcode"].data_type == "varchar(5)"

    sqlmesh_model = rendered.to_sqlmesh(context, {}, {})
    assert str(sqlmesh_model.query) == model.sql
    assert str(sqlmesh_model.render_query()) == "SELECT * FROM raw.baz AS baz"
    assert sqlmesh_model.columns_to_types == column_types_to_sqlmesh(rendered.columns)
