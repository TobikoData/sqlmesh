import logging
from datetime import datetime, timezone

from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from croniter import croniter_range

from sqlmesh.schedulers.airflow import common, util

logger = logging.getLogger(__name__)


class HighWaterMarkSensor(BaseSensorOperator):
    def __init__(
        self,
        target_dag_id: str,
        target_cron: str,
        this_cron: str,
        target_start_date: datetime = common.AIRFLOW_START_DATE,
        this_start_date: datetime = common.AIRFLOW_START_DATE,
        poke_interval: float = 60.0,
        timeout: float = 7.0 * 24.0 * 60.0 * 60.0,  # 7 days
        mode: str = "reschedule",
        **kwargs,
    ):
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            **kwargs,
        )
        self.target_dag_id = target_dag_id
        self.target_cron = target_cron
        self.this_cron = this_cron
        self.target_start_date = target_start_date.replace(tzinfo=timezone.utc)
        self.this_start_date = this_start_date.replace(tzinfo=timezone.utc)

    def poke(self, context: Context) -> bool:
        task_instance = context["task_instance"]
        dag_run = context["dag_run"]

        target_high_water_mark = self._compute_target_high_water_mark(dag_run)
        current_high_water_mark = util.safe_utcfromtimestamp(
            task_instance.xcom_pull(
                dag_id=self.target_dag_id, key=common.HWM_UTC_XCOM_KEY
            )
        )
        logger.info(
            "The current high water mark for DAG '%s' is '%s' (target is '%s')",
            self.target_dag_id,
            current_high_water_mark,
            target_high_water_mark,
        )
        if current_high_water_mark is not None:
            return current_high_water_mark >= target_high_water_mark
        return False

    def _compute_target_high_water_mark(self, dag_run: DagRun) -> datetime:
        execution_date = dag_run.execution_date.replace(tzinfo=timezone.utc)
        target_prev = next(
            croniter_range(execution_date, self.target_start_date, self.target_cron)
        )
        this_prev = next(
            croniter_range(execution_date, self.this_start_date, self.this_cron)
        )
        return min(target_prev, this_prev)
