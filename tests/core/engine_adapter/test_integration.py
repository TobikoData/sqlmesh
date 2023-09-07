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


class Formatter:
    def __init__(
        self,
        test_type: str,
        engine_adapter: EngineAdapter,
        columns_to_types: t.Dict[str, t.Union[str, exp.DataType]],
    ):
        self.test_type = test_type
        self.engine_adapter = engine_adapter
        self.columns_to_types = {k: exp.DataType.build(v) for k, v in columns_to_types.items()}

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
        return self._format_df(data)

    def output_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._format_df(
            data, include_tz=self.engine_adapter.dialect in ["spark", "databricks"]
        )

    def table(self, table_name: str, schema: str = TEST_SCHEMA) -> exp.Table:
        return normalize_identifiers(
            exp.to_table(".".join([schema, table_name]), dialect=self.engine_adapter.dialect),
            dialect=self.engine_adapter.dialect,
        )

    def _format_df(self, data: pd.DataFrame, include_tz: bool = False) -> pd.DataFrame:
        for timestamp_column in self.timestamp_columns:
            if timestamp_column in data.columns:
                data[timestamp_column] = pd.to_datetime(data[timestamp_column], utc=include_tz)
        return data


class MetadataResults(PydanticModel):
    tables: t.List[str] = []
    views: t.List[str] = []

    @classmethod
    def from_data_objects(cls, data_objects: t.List[DataObject]) -> MetadataResults:
        tables = []
        views = []
        for obj in data_objects:
            if obj.type.is_table:
                tables.append(obj.name)
            elif obj.type.is_view:
                views.append(obj.name)
            else:
                raise ValueError(f"Unexpected object type: {obj.type}")
        return MetadataResults(tables=tables, views=views)

    @property
    def non_temp_tables(self) -> t.List[str]:
        return [x for x in self.tables if not x.startswith("__temp")]


@pytest.fixture(params=["df", "query"])
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
    ]
)
def engine_adapter(request, config) -> EngineAdapter:
    gateway = f"test__{request.param}"
    if gateway not in config.gateways:
        # TODO: Once everything is fully setup we want to error if a gateway is not configured that we expect
        pytest.skip(f"Gateway {gateway} not configured")
    engine_adapter = config.gateways[gateway].connection.create_engine_adapter()
    engine_adapter.DEFAULT_BATCH_SIZE = 1
    schema = normalize_identifiers(
        exp.to_identifier(TEST_SCHEMA), dialect=engine_adapter.dialect
    ).sql(dialect=engine_adapter.dialect)
    engine_adapter.drop_schema(schema, ignore_if_not_exists=True, cascade=True)
    engine_adapter.create_schema(schema)
    return engine_adapter


def compare_dfs(
    actual: t.Union[pd.DataFrame, exp.Expression, str],
    expected: pd.DataFrame,
) -> None:
    def replace_nat_with_none(df):
        return [
            col if type(col) != NaTType else None
            for row in sorted(list(df.itertuples(index=False, name=None)))
            for col in row
        ]

    assert replace_nat_with_none(actual) == replace_nat_with_none(expected)


def create_metadata_results(engine_adapter, schema: str = TEST_SCHEMA) -> MetadataResults:
    return MetadataResults.from_data_objects(engine_adapter._get_data_objects(schema))


def get_current_data(engine_adapter, table_name: exp.Table) -> pd.DataFrame:
    return engine_adapter.fetchdf(exp.select("*").from_(table_name))


@pytest.fixture
def default_columns_to_types():
    return {"id": exp.DataType.build("int"), "ds": exp.DataType.build("string")}


def get_table(
    table_name: str, engine_adapter: EngineAdapter, schema: str = TEST_SCHEMA
) -> exp.Table:
    return normalize_identifiers(
        exp.to_table(".".join([schema, table_name]), dialect=engine_adapter.dialect),
        dialect=engine_adapter.dialect,
    )


def test_temp_table(engine_adapter, test_type, default_columns_to_types):
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    table = formatter.table("example")
    with engine_adapter.temp_table(
        formatter.input_data(input_data), table.sql(dialect=engine_adapter.dialect)
    ) as table_name:
        results = create_metadata_results(engine_adapter)
        assert len(results.views) == 0
        assert len(results.tables) == 1
        assert len(results.non_temp_tables) == 0
        compare_dfs(get_current_data(engine_adapter, table_name), formatter.output_data(input_data))
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == len(results.tables) == len(results.non_temp_tables) == 0


def test_ctas(engine_adapter, test_type, default_columns_to_types):
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    table = formatter.table("test_table")
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    engine_adapter.ctas(table, formatter.input_data(input_data))
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(input_data))


def test_create_view(engine_adapter, test_type, default_columns_to_types):
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    view = formatter.table("test_view")
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    if engine_adapter.dialect == "redshift" and test_type == "df":
        with pytest.raises(
            NotImplementedError,
            match=r"DataFrames are not supported for Redshift views because Redshift doesn't support using `VALUES` in a `CREATE VIEW` statement.",
        ):
            engine_adapter.create_view(view, formatter.input_data(input_data))
        return
    engine_adapter.create_view(view, formatter.input_data(input_data))
    results = create_metadata_results(engine_adapter)
    assert len(results.tables) == 0
    assert len(results.views) == 1
    assert results.views[0] == view.name
    compare_dfs(get_current_data(engine_adapter, view), formatter.output_data(input_data))


def test_replace_query(engine_adapter, test_type, default_columns_to_types):
    # Replace query doesn't support batched queries so we enforce that here
    engine_adapter.DEFAULT_BATCH_SIZE = sys.maxsize
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    table = formatter.table("test_table")
    # Initial Load
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    engine_adapter.create_table(table, formatter.columns_to_types)
    engine_adapter.replace_query(
        table,
        formatter.input_data(input_data),
        # Spark based engines do a create table -> insert overwrite instead of replace. If columns to types aren't
        # provided then it checks the table itself for types. This is fine within SQLMesh since we always know the tables
        # exist prior to evaluation but when running these tests that isn't the case. As a result we just pass in
        # columns_to_types for these two engines so we can still test inference on the other ones
        columns_to_types=formatter.columns_to_types
        if engine_adapter.dialect in ["spark", "databricks"]
        else None,
    )
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(input_data))

    # Replace that we only need to run once
    if type == "df":
        replace_data = pd.DataFrame(
            [
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
                {"id": 6, "ds": "2022-01-06"},
            ]
        )
        engine_adapter.replace_query(
            table,
            formatter.input_data(replace_data),
            columns_to_types=formatter.columns_to_types
            if engine_adapter.dialect in ["spark", "databricks"] and test_type == "query"
            else None,
        )
        results = create_metadata_results(engine_adapter)
        assert len(results.views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(replace_data))


def test_insert_append(engine_adapter, test_type, default_columns_to_types):
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    table = formatter.table("test_table")
    engine_adapter.create_table(table, default_columns_to_types)
    # Initial Load
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    engine_adapter.insert_append(table, formatter.input_data(input_data))
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(input_data))

    # Replace that we only need to run once
    if type == "df":
        append_data = pd.DataFrame(
            [
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
                {"id": 6, "ds": "2022-01-06"},
            ]
        )
        engine_adapter.insert_append(table, formatter.input_data(append_data))
        results = create_metadata_results(engine_adapter)
        assert len(results.views) == 0
        assert len(results.tables) in [1, 2, 3]
        assert len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        compare_dfs(
            get_current_data(engine_adapter, table),
            formatter.output_data(pd.concat([input_data, append_data])),
        )


def test_insert_overwrite_by_time_partition(engine_adapter, test_type, default_columns_to_types):
    ds_type = "timestamp" if engine_adapter.dialect == "bigquery" else "string"
    formatter = Formatter(test_type, engine_adapter, columns_to_types={"id": "int", "ds": ds_type})
    table = formatter.table("test_table")
    if engine_adapter.dialect == "bigquery":
        partitioned_by = ["DATE(ds)"]
    else:
        partitioned_by = formatter.partitioned_by
    engine_adapter.create_table(
        table,
        formatter.columns_to_types,
        partitioned_by=partitioned_by,
        partition_interval_unit="DAY",
    )
    input_data = pd.DataFrame(
        [
            {"id": 1, formatter.time_column: "2022-01-01"},
            {"id": 2, formatter.time_column: "2022-01-02"},
            {"id": 3, formatter.time_column: "2022-01-03"},
        ]
    )
    engine_adapter.insert_overwrite_by_time_partition(
        table,
        formatter.input_data(input_data),
        start="2022-01-02",
        end="2022-01-03",
        time_formatter=formatter.time_formatter,
        time_column=formatter.time_column,
        columns_to_types=formatter.columns_to_types,
    )
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(input_data.iloc[1:]))

    if test_type == "df":
        overwrite_data = pd.DataFrame(
            [
                {"id": 10, formatter.time_column: "2022-01-03"},
                {"id": 4, formatter.time_column: "2022-01-04"},
                {"id": 5, formatter.time_column: "2022-01-05"},
            ]
        )
        engine_adapter.insert_overwrite_by_time_partition(
            table,
            formatter.input_data(overwrite_data),
            start="2022-01-03",
            end="2022-01-05",
            time_formatter=formatter.time_formatter,
            time_column=formatter.time_column,
            columns_to_types=formatter.columns_to_types,
        )
        results = create_metadata_results(engine_adapter)
        assert len(results.views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        compare_dfs(
            get_current_data(engine_adapter, table),
            formatter.output_data(
                pd.DataFrame(
                    [
                        {"id": 2, formatter.time_column: "2022-01-02"},
                        {"id": 10, formatter.time_column: "2022-01-03"},
                        {"id": 4, formatter.time_column: "2022-01-04"},
                        {"id": 5, formatter.time_column: "2022-01-05"},
                    ]
                )
            ),
        )


def test_merge(engine_adapter, test_type, default_columns_to_types):
    if engine_adapter.dialect == "redshift":
        pytest.skip("Redshift currently doesn't support `MERGE`")
    formatter = Formatter(test_type, engine_adapter, default_columns_to_types)
    table = formatter.table("test_table")
    engine_adapter.create_table(table, formatter.columns_to_types)
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    engine_adapter.merge(
        table,
        formatter.input_data(input_data),
        columns_to_types=None,
        unique_key=["id"],
    )
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(get_current_data(engine_adapter, table), formatter.output_data(input_data))

    if test_type == "df":
        merge_data = pd.DataFrame(
            [
                {"id": 2, "ds": "2022-01-10"},
                {"id": 4, "ds": "2022-01-04"},
                {"id": 5, "ds": "2022-01-05"},
            ]
        )
        engine_adapter.merge(
            table,
            formatter.input_data(merge_data),
            columns_to_types=None,
            unique_key=["id"],
        )
        results = create_metadata_results(engine_adapter)
        assert len(results.views) == 0
        assert len(results.tables) == len(results.non_temp_tables) == 1
        assert results.non_temp_tables[0] == table.name
        compare_dfs(
            get_current_data(engine_adapter, table),
            formatter.output_data(
                pd.DataFrame(
                    [
                        {"id": 1, "ds": "2022-01-01"},
                        {"id": 2, "ds": "2022-01-10"},
                        {"id": 3, "ds": "2022-01-03"},
                        {"id": 4, "ds": "2022-01-04"},
                        {"id": 5, "ds": "2022-01-05"},
                    ]
                )
            ),
        )


def test_scd_type_2(engine_adapter, test_type):
    formatter = Formatter(
        test_type,
        engine_adapter,
        columns_to_types={
            "id": "int",
            "name": "string",
            "updated_at": "timestamp",
            "valid_from": "timestamp",
            "valid_to": "timestamp",
        },
    )
    table = formatter.table("test_table")
    input_schema = {
        k: v for k, v in formatter.columns_to_types.items() if k not in ("valid_from", "valid_to")
    }
    engine_adapter.create_table(table, formatter.columns_to_types)
    input_data = pd.DataFrame(
        [
            {"id": 1, "name": "a", "updated_at": "2022-01-01 00:00:00"},
            {"id": 2, "name": "b", "updated_at": "2022-01-02 00:00:00"},
            {"id": 3, "name": "c", "updated_at": "2022-01-03 00:00:00"},
        ]
    )
    engine_adapter.scd_type_2(
        table,
        formatter.input_data(input_data, input_schema),
        unique_key=["id"],
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        updated_at_name="updated_at",
        execution_time="2023-01-01",
        columns_to_types=input_schema,
    )
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(
        get_current_data(engine_adapter, table),
        formatter.output_data(
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
            )
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
    engine_adapter.scd_type_2(
        table,
        formatter.input_data(current_data, input_schema),
        unique_key=["id"],
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        updated_at_name="updated_at",
        execution_time="2023-01-05",
        columns_to_types=input_schema,
    )
    results = create_metadata_results(engine_adapter)
    assert len(results.views) == 0
    assert len(results.tables) == len(results.non_temp_tables) == 1
    assert results.non_temp_tables[0] == table.name
    compare_dfs(
        get_current_data(engine_adapter, table),
        formatter.output_data(
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
            )
        ),
    )
