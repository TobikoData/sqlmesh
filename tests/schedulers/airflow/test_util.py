import pytest

from sqlmesh.schedulers.airflow.util import truncate_task_id_if_needed
from sqlmesh.utils.errors import SQLMeshError


@pytest.mark.parametrize(
    "dag_id, task_id, max_length, expected",
    [
        ("dag_id", "task_id", 100, "task_id"),
        ("dag_id", "task_id", 8, "d"),
        ("dag_id", "task_id", 11, "k_id"),
    ],
)
def test_truncate_task_id_if_needed(dag_id, task_id, max_length, expected, caplog):
    truncated_task_id = truncate_task_id_if_needed(dag_id, task_id, max_length=max_length)
    assert truncated_task_id == expected
    log_msg = f"The dag id '{dag_id}' and task id '{task_id}' is too long for statsd and therefore task_id will be truncated to '{truncated_task_id}'"
    if task_id != truncated_task_id:
        assert log_msg in caplog.text
    else:
        assert log_msg not in caplog.text


def test_truncate_task_id_if_needed_raises():
    with pytest.raises(
        SQLMeshError,
        match="The dag id 'dag_id' is too long for statsd and therefore task_id cannot be truncated",
    ):
        truncate_task_id_if_needed("dag_id", "task_id", max_length=7)
