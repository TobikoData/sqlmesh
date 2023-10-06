from __future__ import annotations

import pathlib
import sys
import typing as t

import pandas as pd
import pytest
from pandas._libs import NaTType
from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh import Config, EngineAdapter
from sqlmesh.core.config import load_config_from_paths
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.utils.date import to_ds
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import Query


TEST_SCHEMA = "test_schema"


class TestContext:
    def __init__(
        self,
        test_type: str,
        engine_adapter: EngineAdapter,
        columns_to_types: t.Optional[t.Dict[str, t.Union[str, exp.DataType]]] = None,
    ):
        self.test_type = test_type
        self.engine_adapter = engine_adapter
        self._columns_to_types = columns_to_types

    @property
    def columns_to_types(self):
        if self._columns_to_types is None:
            self._columns_to_types = {
                "id": exp.DataType.build("int"),
                "ds": exp.DataType.build("string"),
            }
        return self._columns_to_types

    @columns_to_types.setter
    def columns_to_types(self, value: t.Dict[str, t.Union[str, exp.DataType]]):
        self._columns_to_types = {k: exp.DataType.build(v) for k, v in value.items()}

    @property
    def time_columns(self) -> t.List[str]:
        return [
            k
            for k, v in self.columns_to_types.items()
            if v.sql().lower().startswith("timestamp")
            or v.sql().lower().startswith("date")
            or k.lower() == "ds"
        ]

    @property
    def timestamp_columns(self) -> t.List[str]:
        return [
            k for k, v in self.columns_to_types.items() if v.sql().lower().startswith("timestamp")
        ]

    @property
    def time_column(self) -> str:
        return self.time_columns[0]

    @property
    def time_formatter(self) -> t.Callable:
        return lambda x, _: exp.Literal.string(to_ds(x))

    @property
    def partitioned_by(self) -> t.List[exp.Expression]:
        return [parse_one(self.time_column)]

    @property
    def dialect(self) -> str:
        return self.engine_adapter.dialect

    @classmethod
    def _compare_dfs(
        cls, actual: t.Union[pd.DataFrame, exp.Expression, str], expected: pd.DataFrame
    ) -> None:
        def replace_nat_with_none(df):
            return [
                col if type(col) != NaTType else None
                for row in sorted(list(df.itertuples(index=False, name=None)))
                for col in row
            ]

        assert replace_nat_with_none(actual) == replace_nat_with_none(expected)

    def get_metadata_results(self, schema: str = TEST_SCHEMA) -> MetadataResults:
        return MetadataResults.from_data_objects(self.engine_adapter._get_data_objects(schema))

    def _init_engine_adapter(self) -> None:
        schema = normalize_identifiers(
            exp.to_identifier(TEST_SCHEMA), dialect=self.engine_adapter.dialect
        ).sql(dialect=self.engine_adapter.dialect)
        self.engine_adapter.drop_schema(schema, ignore_if_not_exists=True, cascade=True)
        self.engine_adapter.create_schema(schema)

    def _format_df(self, data: pd.DataFrame, include_tz: bool = False) -> pd.DataFrame:
        for timestamp_column in self.timestamp_columns:
            if timestamp_column in data.columns:
                data[timestamp_column] = pd.to_datetime(data[timestamp_column], utc=include_tz)
        return data

    def init(self):
        if self.test_type == "pyspark" and not hasattr(self.engine_adapter, "is_pyspark_df"):
            pytest.skip(f"Engine adapter {self.engine_adapter} doesn't support pyspark")
        self._init_engine_adapter()

    def input_data(
        self, data: pd.DataFrame, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Union[Query, pd.DataFrame]:
        columns_to_types = columns_to_types or self.columns_to_types
        if self.test_type == "query":
            return self.engine_adapter._values_to_sql(
                list(data.itertuples(index=False, name=None)),
                batch_start=0,
                batch_end=sys.maxsize,
                columns_to_types=columns_to_types,
            )
        elif self.test_type == "pyspark":
            return self.engine_adapter.spark.createDataFrame(data)  # type: ignore
        return self._format_df(data)

    def output_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._format_df(data, include_tz=self.dialect in ("spark", "databricks"))

    def table(self, table_name: str, schema: str = TEST_SCHEMA) -> exp.Table:
        return normalize_identifiers(
            exp.to_table(".".join([schema, table_name]), dialect=self.engine_adapter.dialect),
            dialect=self.engine_adapter.dialect,
        )

    def get_current_data(self, table: exp.Table) -> pd.DataFrame:
        return self.engine_adapter.fetchdf(exp.select("*").from_(table))

    def compare_with_current(self, table: exp.Table, expected: pd.DataFrame) -> None:
        self._compare_dfs(self.get_current_data(table), self.output_data(expected))


class MetadataResults(PydanticModel):
    tables: t.List[str] = []
    views: t.List[str] = []
    materialized_views: t.List[str] = []

    @classmethod
    def from_data_objects(cls, data_objects: t.List[DataObject]) -> MetadataResults:
        tables = []
        views = []
        materialized_views = []
        for obj in data_objects:
            if obj.type.is_table:
                tables.append(obj.name)
            elif obj.type.is_view:
                views.append(obj.name)
            elif obj.type.is_materialized_view:
                materialized_views.append(obj.name)
            else:
                raise ValueError(f"Unexpected object type: {obj.type}")
        return MetadataResults(tables=tables, views=views, materialized_views=materialized_views)

    @property
    def non_temp_tables(self) -> t.List[str]:
        return [x for x in self.tables if not x.startswith("__temp")]


@pytest.fixture(params=["df", "query", "pyspark"])
def test_type(request):
    return request.param


@pytest.fixture(scope="session")
def config() -> Config:
    return load_config_from_paths(
        project_paths=[pathlib.Path("examples/wursthall/config.yaml")],
        personal_paths=[pathlib.Path("~/.sqlmesh/config.yaml").expanduser()],
    )


@pytest.fixture(
    params=[
        "duckdb",
        pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("mysql", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("bigquery", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("databricks", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("redshift", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("snowflake", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
        pytest.param("mssql", marks=[pytest.mark.integration, pytest.mark.engine_integration]),
    ]
)
def engine_adapter(request, config) -> EngineAdapter:
    gateway = f"inttest_{request.param}"
    if gateway not in config.gateways:
        # TODO: Once everything is fully setup we want to error if a gateway is not configured that we expect
        pytest.skip(f"Gateway {gateway} not configured")
    engine_adapter = config.gateways[gateway].connection.create_engine_adapter()
    engine_adapter.DEFAULT_BATCH_SIZE = 1
    return engine_adapter


@pytest.fixture
def default_columns_to_types():
    return {"id": exp.DataType.build("int"), "ds": exp.DataType.build("string")}


@pytest.fixture
def ctx(engine_adapter, test_type):
    return TestContext(test_type, engine_adapter)


def test_temp_table(ctx: TestContext):
    ctx.init()
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    table = ctx.table("example")
    with ctx.engine_adapter.temp_table(
        ctx.input_data(input_data), table.sql(dialect=ctx.dialect)
    ) as table_name:
        results = ctx.get_metadata_results()
        assert len(results.views) == 0
        assert len(results.tables) == 1
        assert len(results.non_temp_tables) == 0
        assert len(results.materialized_views) == 0
        ctx.compare_with_current(table_name, input_data)
    results = ctx.get_metadata_results()
    assert len(results.views) == len(results.tables) == len(results.non_temp_tables) == 0


def test_ctas(ctx: TestContext):
    ctx.init()
    table = ctx.table("test_table")
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.engine_adapter.ctas(table, ctx.input_data(input_data))
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(table, input_data)


def test_create_view(ctx: TestContext):
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    view = ctx.table("test_view")
    ctx.init()
    ctx.engine_adapter.create_view(view, ctx.input_data(input_data))
    results = ctx.get_metadata_results()
    assert len(results.tables) == 0
    assert len(results.views) == 1
    assert len(results.materialized_views) == 0
    assert results.views[0] == view.name
    ctx.compare_with_current(view, input_data)


def test_materialized_view(ctx: TestContext):
    if not ctx.engine_adapter.SUPPORTS_MATERIALIZED_VIEWS:
        pytest.skip(f"Engine adapter {ctx.engine_adapter} doesn't support materialized views")
    if ctx.engine_adapter.dialect == "databricks":
        pytest.skip(
            "Databricks requires DBSQL Serverless or Pro warehouse to test materialized views which we do not have setup"
        )
    if ctx.engine_adapter.dialect == "snowflake":
        pytest.skip("Snowflake requires enterprise edition which we do not have setup")
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.init()
    source_table = ctx.table("source_table")
    ctx.engine_adapter.ctas(source_table, ctx.input_data(input_data), ctx.columns_to_types)
    view = ctx.table("test_view")
    view_query = exp.select(*ctx.columns_to_types).from_(source_table)
    ctx.engine_adapter.create_view(view, view_query, materialized=True)
    results = ctx.get_metadata_results()
    # Redshift considers the underlying dataset supporting materialized views as a table therefore we get 2
    # tables in the result
    if ctx.engine_adapter.dialect == "redshift":
        assert len(results.tables) == 2
    else:
        assert len(results.tables) == 1
    assert len(results.views) == 0
    assert len(results.materialized_views) == 1
    assert results.materialized_views[0] == view.name
    ctx.compare_with_current(view, input_data)
    # Make sure that dropping a materialized view also works
    ctx.engine_adapter.drop_view(view, materialized=True)
    results = ctx.get_metadata_results()
    assert len(results.materialized_views) == 0


def test_replace_query(ctx: TestContext):
    ctx.engine_adapter.DEFAULT_BATCH_SIZE = sys.maxsize
    ctx.init()
    table = ctx.table("test_table")
    # Initial Load
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.engine_adapter.create_table(table, ctx.columns_to_types)
    ctx.engine_adapter.replace_query(
        table,
        ctx.input_data(input_data),
        # Spark based engines do a create table -> insert overwrite instead of replace. If columns to types aren't
        # provided then it checks the table itself for types. This is fine within SQLMesh since we always know the tables
        # exist prior to evaluation but when running these tests that isn't the case. As a result we just pass in
        # columns_to_types for these two engines so we can still test inference on the other ones
        columns_to_types=ctx.columns_to_types if ctx.dialect in ["spark", "databricks"] else None,
    )
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(table, input_data)

    # Replace that we only need to run once
    if type == "df":
        replace_data = pd.DataFrame(
            [
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
                {"id": 6, "ds": "2022-01-06"},
            ]
        )
        ctx.engine_adapter.replace_query(
            table,
            ctx.input_data(replace_data),
            columns_to_types=ctx.columns_to_types
            if ctx.dialect in ["spark", "databricks"]
            else None,
        )
        results = ctx.get_metadata_results()
        assert len(results.views) == 0
        assert len(results.materialized_views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        ctx.compare_with_current(table, replace_data)


def test_insert_append(ctx: TestContext):
    ctx.init()
    table = ctx.table("test_table")
    ctx.engine_adapter.create_table(table, ctx.columns_to_types)
    # Initial Load
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.engine_adapter.insert_append(table, ctx.input_data(input_data))
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(table, input_data)

    # Replace that we only need to run once
    if type == "df":
        append_data = pd.DataFrame(
            [
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
                {"id": 6, "ds": "2022-01-06"},
            ]
        )
        ctx.engine_adapter.insert_append(table, ctx.input_data(append_data))
        results = ctx.get_metadata_results()
        assert len(results.views) == 0
        assert len(results.materialized_views) == 0
        assert len(results.tables) in [1, 2, 3]
        assert len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        ctx.compare_with_current(table, pd.concat([input_data, append_data]))


def test_insert_overwrite_by_time_partition(ctx: TestContext):
    ds_type = "string"
    if ctx.dialect == "bigquery":
        ds_type = "timestamp"
    if ctx.dialect == "tsql":
        ds_type = "varchar(max)"

    ctx.columns_to_types = {"id": "int", "ds": ds_type}
    ctx.init()
    table = ctx.table("test_table")
    if ctx.dialect == "bigquery":
        partitioned_by = ["DATE(ds)"]
    else:
        partitioned_by = ctx.partitioned_by  # type: ignore
    ctx.engine_adapter.create_table(
        table,
        ctx.columns_to_types,
        partitioned_by=partitioned_by,
        partition_interval_unit="DAY",
    )
    input_data = pd.DataFrame(
        [
            {"id": 1, ctx.time_column: "2022-01-01"},
            {"id": 2, ctx.time_column: "2022-01-02"},
            {"id": 3, ctx.time_column: "2022-01-03"},
        ]
    )
    ctx.engine_adapter.insert_overwrite_by_time_partition(
        table,
        ctx.input_data(input_data),
        start="2022-01-02",
        end="2022-01-03",
        time_formatter=ctx.time_formatter,
        time_column=ctx.time_column,
        columns_to_types=ctx.columns_to_types,
    )
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(table, input_data.iloc[1:])

    if test_type == "df":
        overwrite_data = pd.DataFrame(
            [
                {"id": 10, ctx.time_column: "2022-01-03"},
                {"id": 4, ctx.time_column: "2022-01-04"},
                {"id": 5, ctx.time_column: "2022-01-05"},
            ]
        )
        ctx.engine_adapter.insert_overwrite_by_time_partition(
            table,
            ctx.input_data(overwrite_data),
            start="2022-01-03",
            end="2022-01-05",
            time_formatter=ctx.time_formatter,
            time_column=ctx.time_column,
            columns_to_types=ctx.columns_to_types,
        )
        results = ctx.get_metadata_results()
        assert len(results.views) == 0
        assert len(results.materialized_views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        ctx.compare_with_current(
            table,
            pd.DataFrame(
                [
                    {"id": 2, ctx.time_column: "2022-01-02"},
                    {"id": 10, ctx.time_column: "2022-01-03"},
                    {"id": 4, ctx.time_column: "2022-01-04"},
                    {"id": 5, ctx.time_column: "2022-01-05"},
                ]
            ),
        )


def test_merge(ctx: TestContext):
    ctx.init()
    table = ctx.table("test_table")
    ctx.engine_adapter.create_table(table, ctx.columns_to_types)
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.engine_adapter.merge(
        table,
        ctx.input_data(input_data),
        columns_to_types=None,
        unique_key=[exp.to_identifier("id")],
    )
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(table, input_data)

    if test_type == "df":
        merge_data = pd.DataFrame(
            [
                {"id": 2, "ds": "2022-01-10"},
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
            ]
        )
        ctx.engine_adapter.merge(
            table,
            ctx.input_data(merge_data),
            columns_to_types=None,
            unique_key=[exp.to_identifier("id")],
        )
        results = ctx.get_metadata_results()
        assert len(results.views) == 0
        assert len(results.materialized_views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        ctx.compare_with_current(
            table,
            pd.DataFrame(
                [
                    {"id": 1, "ds": "2022-01-01"},
                    {"id": 2, "ds": "2022-01-10"},
                    {"id": 3, "ds": "2022-01-03"},
                    {"id": 4, "ds": "2022-01-04"},
                    {"id": 5, "ds": "2022-01-05"},
                ]
            ),
        )


def test_scd_type_2(ctx: TestContext):
    if ctx.dialect == "tsql":
        pytest.skip(f"MSSQL scd type 2 functionality waiting on sqlglot cte in FROM fix")

    name_type = "varchar(max)" if ctx.dialect == "tsql" else "string"
    ctx.columns_to_types = {
        "id": "int",
        "name": name_type,
        "updated_at": "timestamp",
        "valid_from": "timestamp",
        "valid_to": "timestamp",
    }
    ctx.init()
    table = ctx.table("test_table")
    input_schema = {
        k: v for k, v in ctx.columns_to_types.items() if k not in ("valid_from", "valid_to")
    }
    ctx.engine_adapter.create_table(table, ctx.columns_to_types)
    input_data = pd.DataFrame(
        [
            {"id": 1, "name": "a", "updated_at": "2022-01-01 00:00:00"},
            {"id": 2, "name": "b", "updated_at": "2022-01-02 00:00:00"},
            {"id": 3, "name": "c", "updated_at": "2022-01-03 00:00:00"},
        ]
    )
    ctx.engine_adapter.scd_type_2(
        table,
        ctx.input_data(input_data, input_schema),
        unique_key=[exp.to_identifier("id")],
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        updated_at_name="updated_at",
        execution_time="2023-01-01",
        columns_to_types=input_schema,
    )
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(
        table,
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "a",
                    "updated_at": "2022-01-01 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": pd.NaT,
                },
                {
                    "id": 2,
                    "name": "b",
                    "updated_at": "2022-01-02 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": pd.NaT,
                },
                {
                    "id": 3,
                    "name": "c",
                    "updated_at": "2022-01-03 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": pd.NaT,
                },
            ]
        ),
    )

    if test_type == "query":
        return
    current_data = pd.DataFrame(
        [
            # Change `a` to `x`
            {"id": 1, "name": "x", "updated_at": "2022-01-04 00:00:00"},
            # Delete
            # {"id": 2, "name": "b", "updated_at": "2022-01-02 00:00:00"},
            # No change
            {"id": 3, "name": "c", "updated_at": "2022-01-03 00:00:00"},
            # Add
            {"id": 4, "name": "d", "updated_at": "2022-01-04 00:00:00"},
        ]
    )
    ctx.engine_adapter.scd_type_2(
        table,
        ctx.input_data(current_data, input_schema),
        unique_key=[exp.to_identifier("id")],
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        updated_at_name="updated_at",
        execution_time="2023-01-05",
        columns_to_types=input_schema,
    )
    results = ctx.get_metadata_results()
    assert len(results.views) == 0
    assert len(results.materialized_views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    ctx.compare_with_current(
        table,
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "a",
                    "updated_at": "2022-01-01 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": "2022-01-04 00:00:00",
                },
                {
                    "id": 1,
                    "name": "x",
                    "updated_at": "2022-01-04 00:00:00",
                    "valid_from": "2022-01-04 00:00:00",
                    "valid_to": pd.NaT,
                },
                {
                    "id": 2,
                    "name": "b",
                    "updated_at": "2022-01-02 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": "2023-01-05 00:00:00",
                },
                {
                    "id": 3,
                    "name": "c",
                    "updated_at": "2022-01-03 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": pd.NaT,
                },
                {
                    "id": 4,
                    "name": "d",
                    "updated_at": "2022-01-04 00:00:00",
                    "valid_from": "1970-01-01 00:00:00",
                    "valid_to": pd.NaT,
                },
            ]
        ),
    )
