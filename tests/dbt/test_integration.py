import shutil
import typing as t
from enum import Enum
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from dbt.cli.main import dbtRunner
from freezegun import freeze_time
from sqlglot import exp

from sqlmesh import Context
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.engine_adapter import DuckDBEngineAdapter
from sqlmesh.utils.date import now
from sqlmesh.utils.yaml import YAML

pytestmark = [pytest.mark.dbt, pytest.mark.slow]


class TestType(str, Enum):
    DBT_RUNTIME = "dbt_runtime"
    DBT_ADAPTER = "dbt_adapter"
    SQLMESH = "sqlmesh"

    @property
    def is_sqlmesh(self) -> bool:
        return self == TestType.SQLMESH

    @property
    def is_dbt_runtime(self) -> bool:
        return self == TestType.DBT_RUNTIME

    @property
    def is_dbt_adapter(self) -> bool:
        return self == TestType.DBT_ADAPTER

    @property
    def is_dbt_project(self):
        return self.is_dbt_adapter or self.is_dbt_runtime

    @property
    def is_sqlmesh_runtime(self) -> bool:
        return self.is_sqlmesh or self.is_dbt_adapter


@pytest.fixture(params=list(TestType))
def test_type(request):
    return request.param


@pytest.fixture
def create_scd_type_2_dbt_project(tmp_path):
    def _make_function(include_dbt_adapter_support: bool = False) -> t.Tuple[Path, Path]:
        yaml = YAML()
        dbt_project_dir = tmp_path / "dbt"
        dbt_project_dir.mkdir()
        dbt_snapshot_dir = dbt_project_dir / "snapshots"
        dbt_snapshot_dir.mkdir()
        snapshot_file_contents = """{% snapshot marketing %}

                    {{
                        config(
                          target_schema='sushi',
                          unique_key='customer_id',
                          strategy='timestamp',
                          updated_at='updated_at',
                          invalidate_hard_deletes=True,
                        )
                    }}

                    select * from local.sushi.raw_marketing

                    {% endsnapshot %}"""
        snapshot_file = dbt_snapshot_dir / "marketing.sql"
        with open(snapshot_file, "w") as f:
            f.write(snapshot_file_contents)
        dbt_project_config = {
            "name": "scd_type_2",
            "version": "1.0.0",
            "config-version": 2,
            "profile": "test",
            "snapshot-paths": ["snapshots"],
            "models": {"start": "Jan 1 2020"},
        }
        dbt_project_file = dbt_project_dir / "dbt_project.yml"
        with open(dbt_project_file, "w") as f:
            yaml.dump(dbt_project_config, f)
        dbt_data_dir = tmp_path / "dbt_data"
        dbt_data_dir.mkdir()
        dbt_data_file = dbt_data_dir / "local.db"
        dbt_profile_config = {
            "test": {
                "outputs": {
                    "duckdb": {
                        "type": "duckdb",
                        "path": str(dbt_data_file),
                    }
                },
                "target": "duckdb",
            }
        }
        dbt_profile_file = dbt_project_dir / "profiles.yml"
        with open(dbt_profile_file, "w") as f:
            yaml.dump(dbt_profile_config, f)
        if include_dbt_adapter_support:
            sqlmesh_config_file = dbt_project_dir / "config.py"
            with open(sqlmesh_config_file, "w") as f:
                f.write(
                    """from pathlib import Path

from sqlmesh.core.config import AirflowSchedulerConfig
from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent)


test_config = config"""
                )
        return dbt_project_dir, dbt_data_file

    return _make_function


@pytest.fixture
def create_scd_type_2_sqlmesh_project(tmp_path):
    def _make_function() -> t.Tuple[Path, Path]:
        sushi_root = Path("examples/sushi")
        project_root = tmp_path / "sqlmesh"
        project_root.mkdir()
        shutil.copy(str(sushi_root / "config.py"), str(project_root / "config.py"))
        (project_root / "models").mkdir()
        snapshot_def = """MODEL (
  name sushi.marketing,
  kind SCD_TYPE_2(unique_key customer_id, updated_at_as_valid_from true),
  owner jen,
  cron '@daily',
  grain customer_id,
);

SELECT
    customer_id::INT AS customer_id,
    status::TEXT AS status,
    updated_at::TIMESTAMP AS updated_at
FROM
    sushi.raw_marketing"""
        with open(project_root / "models" / "marketing.sql", "w") as f:
            f.write(snapshot_def)
        data_dir = tmp_path / "sqlm_data"
        data_dir.mkdir()
        data_file = data_dir / "local.db"
        return project_root, data_file

    return _make_function


def test_dbt_scd_type_2(
    tmp_path,
    copy_to_temp_path,
    create_scd_type_2_dbt_project,
    create_scd_type_2_sqlmesh_project,
    test_type: TestType,
):
    def create_target_dataframe(
        values: t.List[t.Tuple[int, str, str, str, t.Optional[str]]]
    ) -> pd.DataFrame:
        return pd.DataFrame(
            np.array(
                values,
                [
                    ("customer_id", "int32"),
                    ("status", "object"),
                    ("updated_at", "datetime64[us]"),
                    ("valid_from", "datetime64[us]"),
                    ("valid_to", "datetime64[us]"),
                ],
            ),
        )

    def create_source_dataframe(values: t.List[t.Tuple[int, str, str]]) -> pd.DataFrame:
        return pd.DataFrame(
            np.array(
                values,
                [
                    ("customer_id", "int32"),
                    ("status", "object"),
                    ("updated_at", "datetime64[us]"),
                ],
            ),
        )

    def replace_source_table(
        adapter: DuckDBEngineAdapter,
        values: t.List[t.Tuple[int, str, str]],
    ):
        df = create_source_dataframe(values)
        adapter.replace_query(
            "sushi.raw_marketing",
            df,
            columns_to_types={
                "customer_id": exp.DataType.build("int"),
                "status": exp.DataType.build("STRING"),
                "updated_at": exp.DataType.build("TIMESTAMP"),
            },
        )

    def compare_dataframes(
        actual: pd.DataFrame, expected: pd.DataFrame, msg: t.Optional[str] = None
    ):
        actual = actual.sort_values(by=actual.columns.to_list()).reset_index(drop=True)
        expected = expected.sort_values(by=expected.columns.to_list()).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual, expected, obj=msg)  # type: ignore

    def normalize_dbt_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"dbt_valid_from": "valid_from", "dbt_valid_to": "valid_to"})
        if test_type.is_dbt_runtime:
            df = df.drop(columns=["dbt_updated_at", "dbt_scd_id"])
            # freezegun doesn't work on dbt timestamp so we check if the time is greater than the year we are checking
            # (2020) and if so we set it to the frozen "now" time
            df["valid_to"] = df["valid_to"].apply(
                lambda x: pd.to_datetime(now()).tz_localize(None) if x.year > 2020 else x
            )
            df["valid_to"] = df["valid_to"].astype("datetime64[us]")
        return df

    def get_current_df(adapter: DuckDBEngineAdapter, *, is_dbt_project: bool = False):
        df = adapter.fetchdf("SELECT * FROM sushi.marketing")
        if is_dbt_project:
            df = normalize_dbt_dataframe(df)
        return df

    project_dir, data_file = (
        create_scd_type_2_sqlmesh_project()
        if test_type.is_sqlmesh
        else create_scd_type_2_dbt_project(include_dbt_adapter_support=test_type.is_dbt_adapter)
    )
    context: t.Union[Context, dbtRunner]
    if test_type.is_dbt_runtime:
        dbt_run_args = [
            "snapshot",
            "--profiles-dir",
            f"{project_dir}",
            "--project-dir",
            f"{project_dir}",
            "--target",
            "duckdb",
        ]
        adapter = t.cast(
            DuckDBEngineAdapter,
            DuckDBConnectionConfig(database=str(data_file)).create_engine_adapter(),
        )
        context = dbtRunner()
        run = partial(context.invoke, dbt_run_args)
    else:
        context = Context(paths=project_dir, config="test_config")
        adapter = t.cast(DuckDBEngineAdapter, context.engine_adapter)
        run = partial(context.run, skip_janitor=True)  # type: ignore

    adapter.create_schema("raw")
    adapter.create_schema("sushi")
    if test_type.is_sqlmesh_runtime:
        replace_source_table(adapter, [])
        with freeze_time("2019-12-31 00:00:00"):
            context.plan("prod", auto_apply=True, no_prompts=True)  # type: ignore
    time_expected_mapping: t.Dict[
        str,
        t.Tuple[
            t.List[t.Tuple[int, str, str]], t.List[t.Tuple[int, str, str, str, t.Optional[str]]]
        ],
    ] = {
        "2020-01-01 00:00:00": (
            [
                (1, "a", "2020-01-01 00:00:00"),
                (2, "b", "2020-01-01 00:00:00"),
                (3, "c", "2020-01-01 00:00:00"),
            ],
            [
                (1, "a", "2020-01-01 00:00:00", "2020-01-01 00:00:00", None),
                (2, "b", "2020-01-01 00:00:00", "2020-01-01 00:00:00", None),
                (3, "c", "2020-01-01 00:00:00", "2020-01-01 00:00:00", None),
            ],
        ),
        "2020-01-02 00:00:00": (
            [
                # Update to "x"
                (1, "x", "2020-01-02 00:00:00"),
                # No Change
                (2, "b", "2020-01-01 00:00:00"),
                # Deleted 3
                # (3, "c", "2020-01-01 00:00:00"),
                # Added 4
                (4, "d", "2020-01-02 00:00:00"),
            ],
            [
                (1, "a", "2020-01-01 00:00:00", "2020-01-01 00:00:00", "2020-01-02 00:00:00"),
                (1, "x", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                (2, "b", "2020-01-01 00:00:00", "2020-01-01 00:00:00", None),
                (3, "c", "2020-01-01 00:00:00", "2020-01-01 00:00:00", "2020-01-02 00:00:00"),
                (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
            ],
        ),
        "2020-01-04 00:00:00": (
            [
                # Update to "y"
                (1, "y", "2020-01-03 00:00:00"),
                # Delete 2
                # (2, "b", "2020-01-01 00:00:00"),
                # Add back 3
                (3, "c", "2020-02-01 00:00:00"),
                # No Change
                (4, "d", "2020-01-02 00:00:00"),
                # Added 5
                (5, "e", "2020-01-03 00:00:00"),
            ],
            [
                (1, "a", "2020-01-01 00:00:00", "2020-01-01 00:00:00", "2020-01-02 00:00:00"),
                (1, "x", "2020-01-02 00:00:00", "2020-01-02 00:00:00", "2020-01-03 00:00:00"),
                (1, "y", "2020-01-03 00:00:00", "2020-01-03 00:00:00", None),
                (2, "b", "2020-01-01 00:00:00", "2020-01-01 00:00:00", "2020-01-04 00:00:00"),
                (
                    3,
                    "c",
                    "2020-01-01 00:00:00",
                    "2020-01-01 00:00:00",
                    "2020-01-04 00:00:00" if test_type.is_dbt_runtime else "2020-01-02 00:00:00",
                ),
                # Since 3 was deleted and came back and the updated at time when it came back
                # is greater than the execution time when it was deleted, we have the valid_from
                # match the updated_at time. If it was less then the valid_from would match the
                # execution time when it was deleted.
                (3, "c", "2020-02-01 00:00:00", "2020-02-01 00:00:00", None),
                # What the result would be if the updated_at time was `2020-01-03`
                # (3, "c", "2020-01-03 00:00:00", to_ts(tomorrow), None),
                (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                (5, "e", "2020-01-03 00:00:00", "2020-01-03 00:00:00", None),
            ],
        ),
    }
    for time, (new_source_data, expected_table_data) in time_expected_mapping.items():
        replace_source_table(adapter, new_source_data)
        with freeze_time(time):
            run()
            df_actual = get_current_df(adapter, is_dbt_project=test_type.is_dbt_project)
            df_expected = create_target_dataframe(expected_table_data)
            compare_dataframes(df_actual, df_expected, msg=f"Failed on time {time}")
