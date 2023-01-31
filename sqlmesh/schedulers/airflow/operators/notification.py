import abc
import typing as t

from airflow.models import BaseOperator

from sqlmesh.core.notification_target import BaseNotificationTarget
from sqlmesh.core.plan import PlanStatus
from sqlmesh.schedulers.airflow import common

NT = t.TypeVar("NT", bound=BaseNotificationTarget)


class BaseNotificationOperatorProvider(abc.ABC, t.Generic[NT]):
    @abc.abstractmethod
    def operator(
        self,
        target: NT,
        plan_status: PlanStatus,
        plan_dag_spec: common.PlanDagSpec,
        **dag_kwargs: t.Any,
    ) -> t.Optional[BaseOperator]:
        pass

    def get_trigger_rule(self, plan_status: PlanStatus) -> str:
        if plan_status.is_failed:
            return "one_failed"
        return "all_success"

    def get_task_id(self, target: NT, plan_status: PlanStatus) -> str:
        return f"plan_{plan_status.value}_{target.type_}_notification"
