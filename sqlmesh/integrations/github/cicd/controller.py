from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib
import re
import typing as t
import unittest
from enum import Enum
from typing import List

from hyperscript import Element, h
from sqlglot.helper import seq_get

from sqlmesh.core import constants as c
from sqlmesh.core.console import SNAPSHOT_CHANGE_CATEGORY_STR, MarkdownConsole
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import parse_model_name
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot.definition import (
    Intervals,
    SnapshotChangeCategory,
    _format_date_time,
    merge_intervals,
)
from sqlmesh.core.user import User
from sqlmesh.integrations.github.shared import PullRequestInfo
from sqlmesh.utils.date import make_inclusive
from sqlmesh.utils.errors import CICDBotError, PlanError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from github import Github
    from github.CheckRun import CheckRun
    from github.Issue import Issue
    from github.PullRequest import PullRequest
    from github.PullRequestReview import PullRequestReview
    from github.Repository import Repository

    from sqlmesh.core.config import Config
    from sqlmesh.core.snapshot import Snapshot

logger = logging.getLogger(__name__)


class GithubCheckStatus(str, Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"

    @property
    def is_queued(self) -> bool:
        return self == GithubCheckStatus.QUEUED

    @property
    def is_in_progress(self) -> bool:
        return self == GithubCheckStatus.IN_PROGRESS

    @property
    def is_completed(self) -> bool:
        return self == GithubCheckStatus.COMPLETED


class GithubCheckConclusion(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    NEUTRAL = "neutral"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    ACTION_REQUIRED = "action_required"
    SKIPPED = "skipped"

    @property
    def is_success(self) -> bool:
        return self == GithubCheckConclusion.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self == GithubCheckConclusion.FAILURE

    @property
    def is_neutral(self) -> bool:
        return self == GithubCheckConclusion.NEUTRAL

    @property
    def is_cancelled(self) -> bool:
        return self == GithubCheckConclusion.CANCELLED

    @property
    def is_timed_out(self) -> bool:
        return self == GithubCheckConclusion.TIMED_OUT

    @property
    def is_action_required(self) -> bool:
        return self == GithubCheckConclusion.ACTION_REQUIRED

    @property
    def is_skipped(self) -> bool:
        return self == GithubCheckConclusion.SKIPPED


class AffectedEnvironmentModel(PydanticModel):
    model_name: str
    view_name: str
    intervals: Intervals
    change_category: SnapshotChangeCategory
    interval_unit: t.Optional[IntervalUnit]

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot) -> AffectedEnvironmentModel:
        assert snapshot.change_category
        return cls(
            model_name=snapshot.model.name,
            view_name=snapshot.model.view_name,
            intervals=snapshot.dev_intervals
            if snapshot.change_category.is_forward_only
            else snapshot.intervals,
            change_category=snapshot.change_category,
        )

    @property
    def formatted_loaded_intervals(self) -> str:
        merged_inclusive_intervals = [
            make_inclusive(start, end) for start, end in merge_intervals(self.intervals)
        ]
        return ", ".join(
            f"({_format_date_time(start, self.interval_unit)} - {_format_date_time(end, self.interval_unit)})"
            for start, end in merged_inclusive_intervals
        )

    @property
    def change_category_str(self) -> str:
        return SNAPSHOT_CHANGE_CATEGORY_STR[self.change_category]


class GithubEvent:
    """
    Takes a Github Actions event payload and provides a simple interface to access
    """

    def __init__(self, payload: t.Dict[str, t.Any]) -> None:
        self.payload = payload
        self._pull_request_info: t.Optional[PullRequestInfo] = None

    @classmethod
    def from_path(cls, path: t.Union[str, pathlib.Path]) -> GithubEvent:
        with open(path) as f:
            return cls(payload=json.load(f))

    @classmethod
    def from_env(cls) -> GithubEvent:
        return cls.from_path(os.environ["GITHUB_EVENT_PATH"])

    @property
    def is_review(self) -> bool:
        return bool(self.payload.get("review"))

    @property
    def is_comment(self) -> bool:
        return bool(self.payload.get("comment"))

    @property
    def is_pull_request(self) -> bool:
        return bool(self.payload.get("pull_request"))

    @property
    def pull_request_url(self) -> str:
        if self.is_review:
            return self.payload["review"]["pull_request_url"]
        if self.is_comment:
            return self.payload["issue"]["pull_request"]["url"]
        if self.is_pull_request:
            return self.payload["pull_request"]["_links"]["self"]["href"]
        raise CICDBotError("Unable to determine pull request url")

    @property
    def pull_request_info(self) -> PullRequestInfo:
        if not self._pull_request_info:
            self._pull_request_info = PullRequestInfo.create_from_pull_request_url(
                self.pull_request_url
            )
        return self._pull_request_info


class GithubController:
    def __init__(
        self,
        paths: t.Union[str, t.Iterable[str]],
        token: str,
        config: t.Optional[t.Union[Config, str]] = None,
        event: t.Optional[GithubEvent] = None,
    ) -> None:
        self._paths = paths
        self._config = config
        self._token = token
        self._event = event or GithubEvent.from_env()
        self._pr_plan: t.Optional[Plan] = None
        self._prod_plan: t.Optional[Plan] = None
        self._console: t.Optional[MarkdownConsole] = None
        self._check_run_mapping: t.Dict[str, CheckRun] = {}
        self.__client: t.Optional[Github] = None
        self.__repo: t.Optional[Repository] = None
        self.__pull_request: t.Optional[PullRequest] = None
        self.__issue: t.Optional[Issue] = None
        self.__reviews: t.Optional[t.Iterable[PullRequestReview]] = None
        self.__approvers: t.Optional[t.Set[str]] = None
        self.__context: t.Optional[Context] = None

    @property
    def _context(self) -> Context:
        if not self.__context:
            self.__context = Context(
                paths=self._paths,
                config=self._config,
            )
        return self.__context

    @property
    def _client(self) -> Github:
        from github import Github

        if not self.__client:
            self.__client = Github(
                base_url=os.environ["GITHUB_API_URL"],
                login_or_token=self._token,
            )
        return self.__client

    @property
    def _repo(self) -> Repository:
        if not self.__repo:
            self.__repo = self._client.get_repo(
                self._event.pull_request_info.full_repo_path, lazy=True
            )
        return self.__repo

    @property
    def _pull_request(self) -> PullRequest:
        if not self.__pull_request:
            self.__pull_request = self._repo.get_pull(self._event.pull_request_info.pr_number)
        return self.__pull_request

    @property
    def _issue(self) -> Issue:
        if not self.__issue:
            self.__issue = self._repo.get_issue(self._event.pull_request_info.pr_number)
        return self.__issue

    @property
    def _reviews(self) -> t.Iterable[PullRequestReview]:
        if not self.__reviews:
            self.__reviews = self._pull_request.get_reviews()
        return self.__reviews

    @property
    def _approvers(self) -> t.Set[str]:
        if not self.__approvers:
            # TODO: The python module says that user names can be None and this is not currently handled
            self.__approvers = {
                review.user.name or "UNKNOWN"
                for review in self._reviews
                if review.state.lower() == "approved"
            }
        return self.__approvers

    @property
    def _required_approvers(self) -> t.List[User]:
        return [user for user in self._context.config.users if user.is_required_approver]

    @property
    def _required_approvers_with_approval(self) -> t.List[User]:
        return [
            user for user in self._required_approvers if user.github_username in self._approvers
        ]

    @property
    def pr_environment_name(self) -> str:
        return Environment.normalize_name(
            "_".join(
                [
                    self._event.pull_request_info.repo,
                    str(self._event.pull_request_info.pr_number),
                ]
            )
        )

    @property
    def do_required_approval_check(self) -> bool:
        """We want to skip required approval check if no users have this role"""
        return bool(self._required_approvers)

    @property
    def has_required_approval(self) -> bool:
        """
        Check if the PR has a required approver.

        TODO: Allow defining requiring some number, or all, required approvers.
        """
        if len(self._required_approvers) == 0:
            return True
        return bool(self._required_approvers_with_approval)

    @property
    def pr_plan(self) -> Plan:
        if not self._pr_plan:
            self._pr_plan = self._context.plan(
                environment=self.pr_environment_name,
                skip_backfill=True,
                auto_apply=False,
                no_prompts=True,
                no_auto_categorization=True,
                skip_tests=True,
            )
        return self._pr_plan

    @property
    def prod_plan(self) -> Plan:
        if not self._prod_plan:
            self._prod_plan = self._context.plan(
                c.PROD,
                auto_apply=False,
                no_gaps=True,
                no_prompts=True,
                no_auto_categorization=True,
                skip_tests=True,
            )
        return self._prod_plan

    @property
    def console(self) -> MarkdownConsole:
        if not self._console:
            self._console = MarkdownConsole()
        return self._console

    def _get_plan_summary(self, plan: Plan) -> str:
        try:
            self.console._show_categorized_snapshots(plan)
            catagorized_snapshots = self.console.consume_captured_output()
            self.console._show_missing_dates(plan)
            missing_dates = self.console.consume_captured_output()
            if not catagorized_snapshots and not missing_dates:
                return "No changes to apply."
            return f"{catagorized_snapshots}\n{missing_dates}"
        except PlanError:
            return "Plan failed to generate. Check for pending or unresolved changes."

    def run_tests(self) -> t.Tuple[unittest.result.TestResult, str]:
        """
        Run tests for the PR
        """
        return self._context._run_tests()

    def update_sqlmesh_comment_info(
        self, value: str, find_regex: t.Optional[str], replace_if_exists: bool = True
    ) -> None:
        """
        Update the SQLMesh PR Comment for the given lookup key with the given value. If a comment does not exist then
        it creates one. It determines the comment to update by looking for a comment with the header. If the lookup key
        already exists in the comment then it will replace the value if replace_if_exists is True, otherwise it will
        not update the comment. If no `find_regex` is provided then it will just append the value to the comment.
        """
        comment_header = "**SQLMesh Bot Info**"
        comment = seq_get(
            [comment for comment in self._issue.get_comments() if comment_header in comment.body],
            0,
        )
        if not comment:
            comment = self._issue.create_comment(comment_header)
        existing_value = seq_get(re.findall(find_regex, comment.body), 0) if find_regex else None
        if existing_value:
            if replace_if_exists:
                comment.edit(re.sub(t.cast(str, find_regex), value, comment.body))
        else:
            comment.edit(f"{comment.body}\n{value}")

    def update_pr_environment(self) -> None:
        """
        Creates a PR environment from the logic present in the PR. If the PR contains changes that are
        uncategorized, then an error will be raised.
        """
        self._context.apply(self.pr_plan)

    def deploy_to_prod(self) -> None:
        """
        Attempts to deploy a plan to prod. If the plan is not up-to-date or has gaps then it will raise.
        """
        # If the PR is already merged then we will not deploy to prod.
        if self._pull_request.merged:
            raise CICDBotError("PR is already merged. Skipping deploy to prod.")
        plan_summary = f"""<details>
  <summary>Prod Plan Being Applied</summary>

{self._get_plan_summary(self.prod_plan)}
</details>

"""
        self.update_sqlmesh_comment_info(
            value=plan_summary,
            find_regex=None,
        )
        self._context.apply(self.prod_plan)

    def delete_pr_environment(self) -> None:
        """
        Deletes all the schemas for a given environment by checking all the schemas used by models.
        """
        schemas = {parse_model_name(model.name)[1] for model in self._context.models.values()}
        for schema in schemas:
            assert schema
            self._context.engine_adapter.drop_schema(
                schema_name="__".join([schema, self.pr_environment_name]),
                ignore_if_not_exists=True,
                cascade=True,
            )
        return

    def get_pr_affected_models(self) -> t.List[AffectedEnvironmentModel]:
        plan = self._context.plan(
            c.PROD,
            auto_apply=False,
            no_gaps=False,
            no_prompts=True,
            no_auto_categorization=True,
            skip_tests=True,
        )
        affected_env_models = []
        for snapshot in plan.directly_modified:
            if not snapshot.change_category:
                continue
            affected_env_models.append(AffectedEnvironmentModel.from_snapshot(snapshot))
            for downstream_indirect in plan.indirectly_modified.get(snapshot.name, set()):
                downstream_snapshot = plan.context_diff.snapshots[downstream_indirect]
                if downstream_snapshot.is_indirect_non_breaking:
                    continue
                affected_env_models.append(
                    AffectedEnvironmentModel.from_snapshot(downstream_snapshot)
                )
        return affected_env_models

    def _update_check(
        self,
        name: str,
        status: GithubCheckStatus,
        title: str,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        summary: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of the merge commit.
        """

        current_time = datetime.datetime.now(datetime.timezone.utc)
        kwargs: t.Dict[str, t.Any] = {
            "name": name,
            # Note: The environment variable `GITHUB_SHA` would be the merge commit so that is why instead we
            # get the last commit on the PR.
            "head_sha": self._pull_request.head.sha,
            "status": status.value,
        }
        if status.is_in_progress:
            kwargs["started_at"] = current_time
        if status.is_completed:
            kwargs["completed_at"] = current_time
        if conclusion:
            kwargs["conclusion"] = conclusion.value
        kwargs["output"] = {"title": title, "summary": summary or title}
        if name in self._check_run_mapping:
            check_run = self._check_run_mapping[name]
            check_run.edit(
                **{k: v for k, v in kwargs.items() if k not in ("name", "head_sha", "started_at")}
            )
        else:
            self._check_run_mapping[name] = self._repo.create_check_run(**kwargs)

    def update_test_check(
        self,
        status: GithubCheckStatus,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        result: t.Optional[unittest.result.TestResult] = None,
        failed_output: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of tests for code in the PR
        """
        status_to_title = {
            GithubCheckStatus.IN_PROGRESS: "Running Tests",
            GithubCheckStatus.QUEUED: "Waiting to Run Tests",
        }
        title = status_to_title.get(status)
        summary = title
        if not title:
            if not result:
                title = summary = "Skipped Tests"
            else:
                if result.wasSuccessful():
                    title = "Test Passed"
                    conclusion = GithubCheckConclusion.SUCCESS
                else:
                    title = "Tests Failed"
                    conclusion = GithubCheckConclusion.FAILURE
                self.console.log_test_results(
                    result, failed_output or "", self._context._test_engine_adapter.dialect
                )
                summary = self.console.consume_captured_output()
        self._update_check(
            name="SQLMesh - Run Unit Tests",
            status=status,
            title=title,
            conclusion=conclusion,
            summary=summary,
        )

    def update_required_approval_check(
        self, status: GithubCheckStatus, conclusion: t.Optional[GithubCheckConclusion] = None
    ) -> None:
        """
        Updates the status of the merge commit for the required approval.
        """
        status_to_title = {
            GithubCheckStatus.IN_PROGRESS: "Checking if we have required Approvers",
            GithubCheckStatus.QUEUED: "Waiting to Check if we have required Approvers",
        }
        title = status_to_title.get(status)
        if not title:
            assert conclusion
            conclusion_to_title = {
                GithubCheckConclusion.SUCCESS: f"Obtained approval from required approvers: {', '.join([user.github_username or user.username for user in self._required_approvers_with_approval])}",
            }
            title = conclusion_to_title.get(conclusion, "Need a Required Approval")
        summary = f"**List of possible required approvers:**\n"
        for user in self._required_approvers:
            summary += f"- `{user.github_username or user.username}`\n"
        self._update_check(
            name="SQLMesh - Has Required Approval",
            status=status,
            conclusion=conclusion,
            title=t.cast(str, title),
            summary=summary,
        )

    def update_pr_environment_check(
        self, status: GithubCheckStatus, conclusion: t.Optional[GithubCheckConclusion] = None
    ) -> None:
        """
        Updates the status of the merge commit for the PR environment.
        """
        title = f"PR Virtual Data Environment: {self.pr_environment_name}"
        status_to_summary = {
            GithubCheckStatus.QUEUED: f":pause_button: Waiting to create or update PR Environment `{self.pr_environment_name}`",
            GithubCheckStatus.IN_PROGRESS: f":rocket: Creating or Updating PR Environment `{self.pr_environment_name}`",
        }
        summary = status_to_summary.get(status)
        if summary:
            self._update_check(
                name="SQLMesh - PR Environment Synced",
                status=status,
                conclusion=conclusion,
                title=title,
                summary=summary,
            )
            return
        assert conclusion
        if conclusion.is_success:
            pr_affected_models = self.get_pr_affected_models()
            if not pr_affected_models:
                summary = "No models were modified in this PR.\n"
            else:
                header_rows = [
                    h("th", {"colspan": "3"}, "PR Environment Summary"),
                    [
                        h("th", "Model"),
                        h("th", "Change Type"),
                        h("th", "Dates Loaded"),
                    ],
                ]
                body_rows: List[Element | List[Element]] = []
                for affected_model in pr_affected_models:
                    model_rows = [
                        h("td", affected_model.model_name),
                        h("td", affected_model.change_category_str),
                    ]
                    if affected_model.intervals:
                        model_rows.append(h("td", affected_model.formatted_loaded_intervals))
                    else:
                        model_rows.append(h("td", "N/A"))
                    body_rows.append(model_rows)
                table_header = h("thead", [h("tr", row) for row in header_rows])
                table_body = h("tbody", [h("tr", row) for row in body_rows])
                summary = str(h("table", [table_header, table_body]))
            self._update_check(
                name="SQLMesh - PR Environment Synced",
                status=status,
                conclusion=conclusion,
                title=title,
                summary=summary,
            )
            self.update_sqlmesh_comment_info(
                value=f"- PR Virtual Data Environment: `{self.pr_environment_name}`",
                find_regex=r"- PR Virtual Data Environment: `.*`",
                replace_if_exists=False,
            )
        else:
            conclusion_to_summary = {
                GithubCheckConclusion.SKIPPED: f":next_track_button: Skipped creating or updating PR Environment `{self.pr_environment_name}` since a prior stage failed",
                GithubCheckConclusion.FAILURE: f":x: Failed to create or update PR Environment `{self.pr_environment_name}`. There are likely uncateogrized changes. Run `plan` to apply these changes.",
                GithubCheckConclusion.CANCELLED: f":stop_sign: Cancelled creating or updating PR Environment `{self.pr_environment_name}`",
                GithubCheckConclusion.ACTION_REQUIRED: f":warning: Action Required to create or update PR Environment `{self.pr_environment_name}`. There are likely uncateogrized changes. Run `plan` to apply these changes.",
            }
            summary = conclusion_to_summary.get(
                conclusion, f":interrobang: Got an unexpected conclusion: {conclusion.value}"
            )
            self._update_check(
                name="SQLMesh - PR Environment Synced",
                status=status,
                conclusion=conclusion,
                title=title,
                summary=summary,
            )

    def update_prod_environment_check(
        self, status: GithubCheckStatus, conclusion: t.Optional[GithubCheckConclusion] = None
    ) -> None:
        """
        Updates the status of the merge commit for the prod environment.
        """
        status_to_title = {
            GithubCheckStatus.IN_PROGRESS: "Deploying to Prod",
            GithubCheckStatus.QUEUED: "Waiting to see if we can deploy to prod",
        }
        title = summary = status_to_title.get(status)
        if not title:
            assert conclusion
            conclusion_to_title = {
                GithubCheckConclusion.SUCCESS: "Deployed to Prod",
                GithubCheckConclusion.CANCELLED: "Cancelled deploying to prod",
                GithubCheckConclusion.SKIPPED: "Skipped deploying to prod since dependencies were not met",
                GithubCheckConclusion.FAILURE: "Failed to deploy to prod",
            }
            title = conclusion_to_title.get(
                conclusion, f"Got an unexpected conclusion: {conclusion.value}"
            )
            summary = "**Preview of Prod Plan**\n"
            summary += self._get_plan_summary(self.prod_plan)
        self._update_check(
            name="SQLMesh - Prod Environment Synced",
            status=status,
            conclusion=conclusion,
            title=t.cast(str, title),
            summary=summary,
        )

    def merge_pr(self) -> None:
        """
        Merges the PR
        """
        self._pull_request.merge()
