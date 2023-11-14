from __future__ import annotations

from sqlmesh.schedulers.airflow import common


def test_snapshot_dag_id():
    assert (
        common.dag_id_for_name_version('"test_schema"."test_table"', "version")
        == "sqlmesh_snapshot__test_schema___test_table__version_dag"
    )
