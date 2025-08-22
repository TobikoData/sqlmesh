import datetime
import shutil
import typing as t
from enum import Enum
from functools import partial
from pathlib import Path

import pandas as pd  # noqa: TID253
import pytest

from sqlmesh.dbt.util import DBT_VERSION

if DBT_VERSION >= (1, 5, 0):
    from dbt.cli.main import dbtRunner  # type: ignore

import time_machine

from sqlmesh import Context
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.engine_adapter import DuckDBEngineAdapter
from sqlmesh.utils.pandas import columns_to_types_from_df
from sqlmesh.utils.yaml import YAML
from tests.utils.pandas import compare_dataframes, create_df

# Some developers had issues with this test freezing locally so we mark it as cicdonly
pytestmark = [pytest.mark.dbt, pytest.mark.slow, pytest.mark.cicdonly]


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


class TestStrategy(str, Enum):
    CHECK = "check"
    TIMESTAMP = "timestamp"

    @property
    def is_timestamp(self) -> bool:
        return self == TestStrategy.TIMESTAMP

    @property
    def is_check(self) -> bool:
        return self == TestStrategy.CHECK


class TestSCDType2:
    source_schema = {"customer_id": "int32", "status": "object", "updated_at": "datetime64[us]"}
    target_schema = {
        "customer_id": "int32",
        "status": "object",
        "updated_at": "datetime64[us]",
        "valid_from": "datetime64[us]",
        "valid_to": "datetime64[us]",
    }

    @pytest.fixture(params=list(TestType))
    def test_type(self, request):
        return request.param

    @pytest.fixture(params=[True, False])
    def invalidate_hard_deletes(self, request):
        return request.param

    @pytest.fixture(scope="function")
    def create_scd_type_2_dbt_project(self, tmp_path):
        def _make_function(
            test_strategy: TestStrategy,
            include_dbt_adapter_support: bool = False,
            invalidate_hard_deletes: bool = False,
        ) -> t.Tuple[Path, Path]:
            yaml = YAML()
            dbt_project_dir = tmp_path / "dbt"
            dbt_project_dir.mkdir()
            dbt_snapshot_dir = dbt_project_dir / "snapshots"
            dbt_snapshot_dir.mkdir()
            snapshot_file_contents = f"""{{% snapshot marketing %}}

                        {{{{
                            config(
                              target_schema='sushi',
                              unique_key='customer_id',
                              strategy='{test_strategy.value}',
                              {"updated_at='updated_at'" if test_strategy.is_timestamp else "check_cols='all'"},
                              invalidate_hard_deletes={invalidate_hard_deletes},
                            )
                        }}}}

                        select * from local.sushi.raw_marketing

                        {{% endsnapshot %}}"""
            snapshot_file = dbt_snapshot_dir / "marketing.sql"
            with open(snapshot_file, "w", encoding="utf-8") as f:
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
            with open(dbt_project_file, "w", encoding="utf-8") as f:
                yaml.dump(dbt_project_config, f)
            dbt_data_dir = tmp_path / "dbt_data"
            dbt_data_dir.mkdir()
            dbt_data_file = dbt_data_dir / "local.db"
            dbt_profile_config = {
                "test": {
                    "outputs": {"duckdb": {"type": "duckdb", "path": str(dbt_data_file)}},
                    "target": "duckdb",
                }
            }
            dbt_profile_file = dbt_project_dir / "profiles.yml"
            with open(dbt_profile_file, "w", encoding="utf-8") as f:
                yaml.dump(dbt_profile_config, f)
            if include_dbt_adapter_support:
                sqlmesh_config_file = dbt_project_dir / "config.py"
                with open(sqlmesh_config_file, "w", encoding="utf-8") as f:
                    f.write(
                        """from pathlib import Path

from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent)


test_config = config"""
                    )
            return dbt_project_dir, dbt_data_file

        return _make_function

    @pytest.fixture
    def create_scd_type_2_sqlmesh_project(self, tmp_path):
        def _make_function(
            test_strategy: TestStrategy, invalidate_hard_deletes: bool
        ) -> t.Tuple[Path, Path]:
            sushi_root = Path("examples/sushi")
            project_root = tmp_path / "sqlmesh"
            project_root.mkdir()
            shutil.copy(str(sushi_root / "config.py"), str(project_root / "config.py"))
            (project_root / "models").mkdir()
            if test_strategy.is_timestamp:
                kind_def = f"SCD_TYPE_2(unique_key customer_id, updated_at_as_valid_from true, invalidate_hard_deletes {invalidate_hard_deletes})"
            else:
                kind_def = f"SCD_TYPE_2_BY_COLUMN(unique_key customer_id, execution_time_as_valid_from true, columns *, invalidate_hard_deletes {invalidate_hard_deletes})"
            snapshot_def = f"""MODEL (
      name sushi.marketing,
      kind {kind_def},
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
            with open(project_root / "models" / "marketing.sql", "w", encoding="utf-8") as f:
                f.write(snapshot_def)
            data_dir = tmp_path / "sqlm_data"
            data_dir.mkdir()
            data_file = data_dir / "local.db"
            return project_root, data_file

        return _make_function

    def _replace_source_table(
        self,
        adapter: DuckDBEngineAdapter,
        values: t.List[t.Tuple[int, str, str]],
    ):
        df = create_df(values, self.source_schema)
        columns_to_types = columns_to_types_from_df(df)

        if values:
            adapter.replace_query(
                "sushi.raw_marketing", df, target_columns_to_types=columns_to_types
            )
        else:
            adapter.create_table("sushi.raw_marketing", target_columns_to_types=columns_to_types)

    def _normalize_dbt_dataframe(
        self,
        df: pd.DataFrame,
        test_type: TestType,
        time_start_end_mapping: t.Optional[
            t.Dict[str, t.Tuple[datetime.datetime, datetime.datetime]]
        ],
    ) -> pd.DataFrame:
        def update_now_column(col_timestamp: pd.Timestamp) -> datetime.datetime:
            assert time_start_end_mapping is not None
            col_datetime = col_timestamp.to_pydatetime()
            for now_time, (start_time, end_time) in time_start_end_mapping.items():
                start_time = start_time.replace(tzinfo=None)
                end_time = end_time.replace(tzinfo=None)
                if start_time <= col_datetime <= end_time:
                    col_datetime = pd.to_datetime(now_time).tz_localize(None)
            return col_datetime

        df = df.rename(columns={"dbt_valid_from": "valid_from", "dbt_valid_to": "valid_to"})
        if test_type.is_dbt_runtime:
            df = df.drop(columns=["dbt_updated_at", "dbt_scd_id"])
            for col in ["valid_from", "valid_to"]:
                df[col] = df[col].apply(update_now_column)
                df[col] = df[col].astype("datetime64[us]")
        return df

    def _get_current_df(
        self,
        adapter: DuckDBEngineAdapter,
        *,
        test_type: TestType,
        time_start_end_mapping: t.Optional[
            t.Dict[str, t.Tuple[datetime.datetime, datetime.datetime]]
        ] = None,
    ) -> pd.DataFrame:
        df = adapter.fetchdf("SELECT * FROM sushi.marketing")
        if test_type.is_dbt_project:
            df = self._normalize_dbt_dataframe(
                df, test_type, time_start_end_mapping=time_start_end_mapping
            )
        return df

    def _get_duckdb_now(self, adapter: DuckDBEngineAdapter) -> datetime.datetime:
        return adapter.fetchone("SELECT now()")[0]  # type: ignore

    def _init_test(
        self,
        create_scd_type_2_dbt_project,
        create_scd_type_2_sqlmesh_project,
        test_type: TestType,
        test_strategy: TestStrategy,
        invalidate_hard_deletes: bool,
    ):
        project_dir, data_file = (
            create_scd_type_2_sqlmesh_project(
                test_strategy=test_strategy, invalidate_hard_deletes=invalidate_hard_deletes
            )
            if test_type.is_sqlmesh
            else create_scd_type_2_dbt_project(
                test_strategy=test_strategy,
                include_dbt_adapter_support=test_type.is_dbt_adapter,
                invalidate_hard_deletes=invalidate_hard_deletes,
            )
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
            self._replace_source_table(adapter, [])
            with time_machine.travel("2019-12-31 00:00:00 UTC"):
                context.plan("prod", auto_apply=True, no_prompts=True)  # type: ignore
        return run, adapter, context

    def test_scd_type_2_by_time(
        self,
        tmp_path,
        copy_to_temp_path,
        create_scd_type_2_dbt_project,
        create_scd_type_2_sqlmesh_project,
        test_type: TestType,
        invalidate_hard_deletes: bool,
    ):
        if test_type.is_dbt_runtime and DBT_VERSION < (1, 5, 0):
            pytest.skip("The dbt version being tested doesn't support the dbtRunner so skipping.")

        run, adapter, context = self._init_test(
            create_scd_type_2_dbt_project,
            create_scd_type_2_sqlmesh_project,
            test_type,
            TestStrategy.TIMESTAMP,
            invalidate_hard_deletes,
        )

        time_expected_mapping: t.Dict[
            str,
            t.Tuple[
                t.List[t.Tuple[int, str, str]], t.List[t.Tuple[int, str, str, str, t.Optional[str]]]
            ],
        ] = {
            "2020-01-01 00:00:00 UTC": (
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
            "2020-01-02 00:00:00 UTC": (
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
                    (
                        3,
                        "c",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00" if invalidate_hard_deletes else None,
                    ),
                    (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                ],
            ),
            "2020-01-04 00:00:00 UTC": (
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
                    (
                        2,
                        "b",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-04 00:00:00" if invalidate_hard_deletes else None,
                    ),
                    (
                        3,
                        "c",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00" if invalidate_hard_deletes else "2020-02-01 00:00:00",
                    ),
                    # Since 3 was deleted and came back and the updated at time when it came back
                    # is greater than the execution time when it was deleted, we have the valid_from
                    # match the updated_at time. If it was less then the valid_from would match the
                    # execution time when it was deleted.
                    (3, "c", "2020-02-01 00:00:00", "2020-02-01 00:00:00", None),
                    # What the result would be if the updated_at time was `2020-01-03`
                    # (3, "c", "2020-01-03 00:00:00", "2020-01-04 00:00:00", None),
                    (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                    (5, "e", "2020-01-03 00:00:00", "2020-01-03 00:00:00", None),
                ],
            ),
        }
        time_start_end_mapping = {}
        for time, (starting_source_data, expected_table_data) in time_expected_mapping.items():
            self._replace_source_table(adapter, starting_source_data)
            # Tick when running dbt runtime because it hangs during execution for unknown reasons.
            with time_machine.travel(time, tick=test_type.is_dbt_runtime):
                start_time = self._get_duckdb_now(adapter)
                run()
                end_time = self._get_duckdb_now(adapter)
                time_start_end_mapping[time] = (start_time, end_time)
                df_actual = self._get_current_df(
                    adapter, test_type=test_type, time_start_end_mapping=time_start_end_mapping
                )
                df_expected = create_df(expected_table_data, self.target_schema)
                compare_dataframes(df_actual, df_expected, msg=f"Failed on time {time}")

    def test_scd_type_2_by_column(
        self,
        tmp_path,
        copy_to_temp_path,
        create_scd_type_2_dbt_project,
        create_scd_type_2_sqlmesh_project,
        test_type: TestType,
        invalidate_hard_deletes: bool,
    ):
        if test_type.is_dbt_runtime:
            pytest.skip(
                "the dbt runner, despite running in an entirely different project with different connection config, reuses the adapter from other tests. As a result we skip this test when running automatically to avoid an error but comment this out if testing column scd type 2 in isolation."
            )
        run, adapter, context = self._init_test(
            create_scd_type_2_dbt_project,
            create_scd_type_2_sqlmesh_project,
            test_type,
            TestStrategy.CHECK,
            invalidate_hard_deletes,
        )

        time_expected_mapping: t.Dict[
            str,
            t.Tuple[
                t.List[t.Tuple[int, str, str]], t.List[t.Tuple[int, str, str, str, t.Optional[str]]]
            ],
        ] = {
            "2020-01-01 00:00:00 UTC": (
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
            "2020-01-02 00:00:00 UTC": (
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
                    (
                        3,
                        "c",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00" if invalidate_hard_deletes else None,
                    ),
                    (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                ],
            ),
            "2020-01-04 00:00:00 UTC": (
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
                    (1, "x", "2020-01-02 00:00:00", "2020-01-02 00:00:00", "2020-01-04 00:00:00"),
                    (1, "y", "2020-01-03 00:00:00", "2020-01-04 00:00:00", None),
                    (
                        2,
                        "b",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-04 00:00:00" if invalidate_hard_deletes else None,
                    ),
                    (
                        3,
                        "c",
                        "2020-01-01 00:00:00",
                        "2020-01-01 00:00:00",
                        "2020-01-02 00:00:00" if invalidate_hard_deletes else "2020-01-04 00:00:00",
                    ),
                    # Since 3 was deleted and came back then the valid_from is set to the execution_time when it
                    # came back.
                    (3, "c", "2020-02-01 00:00:00", "2020-01-04 00:00:00", None),
                    (4, "d", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
                    (5, "e", "2020-01-03 00:00:00", "2020-01-04 00:00:00", None),
                ],
            ),
        }
        time_start_end_mapping = {}
        for time, (starting_source_data, expected_table_data) in time_expected_mapping.items():
            self._replace_source_table(adapter, starting_source_data)
            with time_machine.travel(time, tick=False):
                start_time = self._get_duckdb_now(adapter)
                run()
                end_time = self._get_duckdb_now(adapter)
                time_start_end_mapping[time] = (start_time, end_time)
                df_actual = self._get_current_df(
                    adapter, test_type=test_type, time_start_end_mapping=time_start_end_mapping
                )
                df_expected = create_df(expected_table_data, self.target_schema)
                compare_dataframes(df_actual, df_expected, msg=f"Failed on time {time}")
