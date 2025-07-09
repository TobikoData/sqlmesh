import typing as t
import pytest
from sqlglot import exp
from pytest import FixtureRequest
from sqlmesh.core.engine_adapter import RisingwaveEngineAdapter
from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
)


@pytest.fixture(params=list(generate_pytest_params(ENGINES_BY_NAME["risingwave"])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> RisingwaveEngineAdapter:
    assert isinstance(ctx.engine_adapter, RisingwaveEngineAdapter)
    return ctx.engine_adapter


@pytest.fixture
def risingwave_columns_with_datatypes(ctx: TestContext) -> t.Dict[str, exp.DataType]:
    base_types = {
        "smallint_col": exp.DataType.build(exp.DataType.Type.SMALLINT, nested=False),
        "int_col": exp.DataType.build(exp.DataType.Type.INT, nested=False),
        "bigint_col": exp.DataType.build(exp.DataType.Type.BIGINT, nested=False),
        "ts_col": exp.DataType.build(exp.DataType.Type.TIMESTAMP, nested=False),
        "tstz_col": exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ, nested=False),
        "vchar_col": exp.DataType.build(exp.DataType.Type.VARCHAR, nested=False),
    }
    # generate all arrays of base types
    arr_types = {
        f"{type_name}_arr_col": exp.DataType.build(
            exp.DataType.Type.ARRAY,
            expressions=[base_type],
            nested=True,
        )
        for type_name, base_type in base_types.items()
    }
    # generate struct with all base types as nested columns
    struct_types = {
        "struct_col": exp.DataType.build(
            exp.DataType.Type.STRUCT,
            expressions=[
                exp.ColumnDef(
                    this=exp.Identifier(this=f"nested_{type_name}_col", quoted=False),
                    kind=base_type,
                )
                for type_name, base_type in base_types.items()
            ],
            nested=True,
        )
    }
    return {**base_types, **arr_types, **struct_types}


def test_engine_adapter(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, RisingwaveEngineAdapter)
    assert ctx.engine_adapter.fetchone("select 1") == (1,)


def test_engine_adapter_columns(
    ctx: TestContext, risingwave_columns_with_datatypes: t.Dict[str, exp.DataType]
):
    table = ctx.table("TEST_COLUMNS")
    query = exp.select(
        *[
            exp.cast(exp.null(), dtype).as_(name)
            for name, dtype in risingwave_columns_with_datatypes.items()
        ]
    )
    ctx.engine_adapter.ctas(table, query)

    column_result = ctx.engine_adapter.columns(table)
    assert column_result == risingwave_columns_with_datatypes
