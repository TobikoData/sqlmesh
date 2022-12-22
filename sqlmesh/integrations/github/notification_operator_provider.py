from airflow.models import BaseOperator

from sqlmesh.core.notification_target import NotificationStatus
from sqlmesh.core.plan import PlanStatus
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.integrations.github.shared import add_comment_to_pr
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.operators.notification import (
    BaseNotificationOperatorProvider,
)


class GithubNotificationOperatorProvider(
    BaseNotificationOperatorProvider[GithubNotificationTarget]
):
    """
    Github Notification Operator for Airflow.
    """

    def operator(
        self,
        target: GithubNotificationTarget,
        plan_status: PlanStatus,
        plan_application_request: common.PlanApplicationRequest,
        **dag_kwargs,
    ) -> BaseOperator:
        from airflow.providers.github.operators.github import GithubOperator

        if plan_status.is_started:
            notification_status = NotificationStatus.PROGRESS
            msg = f"Updating Environment `{plan_application_request.environment_name}`"
        elif plan_status.is_finished:
            notification_status = NotificationStatus.SUCCESS
            msg = f"Updated Environment `{plan_application_request.environment_name}`"
        else:
            notification_status = NotificationStatus.FAILURE
            msg = f"Failed to Update Environment `{plan_application_request.environment_name}`"

        return GithubOperator(
            task_id=self.get_task_id(target, plan_status),
            trigger_rule=self.get_trigger_rule(plan_status),
            github_method="get_repo",
            github_method_args={
                "full_name_or_id": target.pull_request_info.full_repo_path
            },
            result_processor=lambda repo: add_comment_to_pr(
                repo,
                target.pull_request_info,
                notification_status,
                msg,
                username_to_append_to="SQLMesh",
            ),
        )
