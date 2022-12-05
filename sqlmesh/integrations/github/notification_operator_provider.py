from airflow.models import BaseOperator

from sqlmesh.core.plan import PlanStatus
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.operators.notification import (
    BaseNotificationOperatorProvider,
)


class GithubNotificationOperatorProvider(
    BaseNotificationOperatorProvider[GithubNotificationTarget]
):
    def operator(
        self,
        target: GithubNotificationTarget,
        plan_status: PlanStatus,
        plan_application_request: common.PlanApplicationRequest,
        **dag_kwargs,
    ) -> BaseOperator:
        from airflow.providers.github.operators.github import GithubOperator
        from github.Repository import Repository

        def post_comment(repo: Repository) -> None:
            issue = repo.get_issue(number=int(target.pull_request_info["pr_number"]))
            issue.create_comment(msg)

        # Basic message until follow up PR
        msg = f"Apply {plan_status.value}"

        return GithubOperator(
            task_id=self.get_task_id(target, plan_status),
            trigger_rule=self.get_trigger_rule(plan_status),
            github_method="get_repo",
            github_method_args={
                "full_name_or_id": target.pull_request_info["full_repo_path"]
            },
            result_processor=post_comment,
        )
