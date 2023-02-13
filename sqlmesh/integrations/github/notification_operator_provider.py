from __future__ import annotations

import typing as t

from sqlmesh.core.notification_target import NotificationStatus
from sqlmesh.core.plan import PlanStatus
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.integrations.github.shared import add_comment_to_pr
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.operators.notification import (
    BaseNotificationOperatorProvider,
)

if t.TYPE_CHECKING:
    from airflow.providers.github.operators.github import GithubOperator


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
        plan_dag_spec: common.PlanDagSpec,
        **dag_kwargs: t.Any,
    ) -> GithubOperator:
        from airflow.providers.github.operators.github import GithubOperator

        if plan_status.is_started:
            notification_status = NotificationStatus.PROGRESS
            msg = f"Updating Environment `{plan_dag_spec.environment_name}`"
        elif plan_status.is_finished:
            notification_status = NotificationStatus.SUCCESS
            msg = f"Updated Environment `{plan_dag_spec.environment_name}`"
        else:
            notification_status = NotificationStatus.FAILURE
            msg = f"Failed to Update Environment `{plan_dag_spec.environment_name}`"

        bot_users = [user for user in plan_dag_spec.users if user.is_bot]
        bot_user_to_append_to = bot_users[0] if bot_users else None

        return GithubOperator(
            task_id=self.get_task_id(target, plan_status),
            trigger_rule=self.get_trigger_rule(plan_status),
            github_method="get_repo",
            github_method_args={"full_name_or_id": target.pull_request_info.full_repo_path},
            result_processor=lambda repo: add_comment_to_pr(
                repo,
                target.pull_request_info,
                notification_status,
                msg,
                user_to_append_to=bot_user_to_append_to,
            ),
        )
