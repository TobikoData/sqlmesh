import logging
import typing as t
from datetime import datetime, timezone

from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.utils.context import Context
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.schedulers.airflow import common, util

logger = logging.getLogger(__name__)


class HighWaterMarkSignalOperator(BaseOperator):
    def __init__(self, target_high_water_mark: t.Optional[datetime] = None, **kwargs):
        super().__init__(**kwargs)
        self.target_high_water_mark = target_high_water_mark

    def execute(self, context: Context) -> None:
        task_instance = context["task_instance"]
        dag_run = context["dag_run"]
        dag_id = context["dag"].dag_id
        self._compare_and_swap_hwm(task_instance, dag_run, dag_id)

    @provide_session
    def _compare_and_swap_hwm(
        self,
        task_instance: TaskInstance,
        dag_run: DagRun,
        dag_id: str,
        session: Session = util.PROVIDED_SESSION,
    ) -> None:
        target_high_water_mark = self.target_high_water_mark or dag_run.execution_date
        target_high_water_mark = target_high_water_mark.replace(tzinfo=timezone.utc)
        current_high_water_mark = util.safe_utcfromtimestamp(
            task_instance.xcom_pull(key=common.HWM_UTC_XCOM_KEY, session=session)
        )
        if (
            current_high_water_mark is None
            or current_high_water_mark < target_high_water_mark
        ):
            logger.info(
                "Setting the high water mark to '%s' for DAG '%s' (previous was '%s')",
                target_high_water_mark,
                dag_id,
                current_high_water_mark,
            )
            task_instance.xcom_push(
                key=common.HWM_UTC_XCOM_KEY,
                value=target_high_water_mark.timestamp(),
                session=session,
            )
        else:
            logger.warning(
                "The current high water mark '%s' for DAG '%s' is ahead of the target '%s'. Ignoring the target",
                current_high_water_mark,
                dag_id,
                target_high_water_mark,
            )
