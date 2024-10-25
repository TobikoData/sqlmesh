from __future__ import annotations

import functools
import json
import logging
import os
import pathlib
import re
import traceback
import typing as t
import unittest
from enum import Enum
from typing import List

import requests
from hyperscript import Element, h
from rich.console import Console
from sqlglot.helper import seq_get

from sqlmesh.core import constants as c
from sqlmesh.core.console import SNAPSHOT_CHANGE_CATEGORY_STR, MarkdownConsole
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import Plan, PlanBuilder
from sqlmesh.core.snapshot.definition import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotTableInfo,
    format_intervals,
)
from sqlmesh.core.user import User
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.utils import word_characters_only
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import (
    CICDBotError,
    NoChangesPlanError,
    PlanError,
    UncategorizedPlanError,
)
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from github import Github
    from github.CheckRun import CheckRun
    from github.Issue import Issue
    from github.IssueComment import IssueComment
    from github.PullRequest import PullRequest
    from github.PullRequestReview import PullRequestReview
    from github.Repository import Repository

    from sqlmesh.core.config import Config

logger = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class PullRequestInfo(PydanticModel):
    """Contains information related to a pull request that can be used to construct other objects/URLs"""

    owner: str
    repo: str
    pr_number: int

    @property
    def full_repo_path(self) -> str:
        return "/".join([self.owner, self.repo])

    @classmethod
    def create_from_pull_request_url(cls, pull_request_url: str) -> PullRequestInfo:
        owner, repo, _, pr_number = pull_request_url.split("/")[-4:]
        return cls(
            owner=owner,
            repo=repo,
            pr_number=pr_number,
        )


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


class MergeStateStatus(str, Enum):
    """
    https://docs.github.com/en/graphql/reference/enums#mergestatestatus
    """

    BEHIND = "behind"
    BLOCKED = "blocked"
    CLEAN = "clean"
    DIRTY = "dirty"
    DRAFT = "draft"
    HAS_HOOKS = "has_hooks"
    UNKNOWN = "unknown"
    UNSTABLE = "unstable"

    @property
    def is_behind(self) -> bool:
        return self == MergeStateStatus.BEHIND

    @property
    def is_blocked(self) -> bool:
        return self == MergeStateStatus.BLOCKED

    @property
    def is_clean(self) -> bool:
        return self == MergeStateStatus.CLEAN

    @property
    def is_dirty(self) -> bool:
        return self == MergeStateStatus.DIRTY

    @property
    def is_draft(self) -> bool:
        return self == MergeStateStatus.DRAFT

    @property
    def is_has_hooks(self) -> bool:
        return self == MergeStateStatus.HAS_HOOKS

    @property
    def is_unknown(self) -> bool:
        return self == MergeStateStatus.UNKNOWN

    @property
    def is_unstable(self) -> bool:
        return self == MergeStateStatus.UNSTABLE


class BotCommand(Enum):
    INVALID = 1
    DEPLOY_PROD = 2

    @classmethod
    def from_comment_body(cls, body: str, namespace: t.Optional[str] = None) -> BotCommand:
        body = body.strip()
        namespace = namespace.strip() if namespace else ""
        input_to_command = {
            namespace + "/deploy": cls.DEPLOY_PROD,
        }
        return input_to_command.get(body, cls.INVALID)

    @property
    def is_invalid(self) -> bool:
        return self == self.INVALID

    @property
    def is_deploy_prod(self) -> bool:
        return self == self.DEPLOY_PROD


class GithubEvent:
    """
    Takes a Github Actions event payload and provides a simple interface to access
    """

    def __init__(self, payload: t.Dict[str, t.Any]) -> None:
        self.payload = payload
        self._pull_request_info: t.Optional[PullRequestInfo] = None

    @classmethod
    def from_obj(cls, obj: t.Dict[str, t.Any]) -> GithubEvent:
        return cls(payload=obj)

    @classmethod
    def from_path(cls, path: t.Union[str, pathlib.Path]) -> GithubEvent:
        with open(pathlib.Path(path), "r", encoding="utf-8") as f:
            return cls.from_obj(json.load(f))

    @classmethod
    def from_env(cls) -> GithubEvent:
        return cls.from_path(os.environ["GITHUB_EVENT_PATH"])

    @property
    def is_review(self) -> bool:
        return bool(self.payload.get("review"))

    @property
    def is_comment(self) -> bool:
        comment = self.payload.get("comment")
        if not comment:
            return False
        if not comment.get("body"):
            return False
        return True

    @property
    def is_comment_added(self) -> bool:
        return self.is_comment and self.payload.get("action") != "deleted"

    @property
    def is_pull_request(self) -> bool:
        return bool(self.payload.get("pull_request"))

    @property
    def is_pull_request_closed(self) -> bool:
        return self.is_pull_request and self.payload.get("action") == "closed"

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

    @property
    def pull_request_comment_body(self) -> t.Optional[str]:
        if self.is_comment_added:
            return self.payload["comment"]["body"]
        return None


class GithubController:
    BOT_HEADER_MSG = ":robot: **SQLMesh Bot Info** :robot:"
    MAX_BYTE_LENGTH = 65535

    def __init__(
        self,
        paths: t.Union[str, t.Iterable[str]],
        token: str,
        config: t.Optional[t.Union[Config, str]] = None,
        event: t.Optional[GithubEvent] = None,
        client: t.Optional[Github] = None,
    ) -> None:
        from github import Github

        logger.debug(f"Initializing GithubController with paths: {paths} and config: {config}")

        self.config = config
        self._paths = paths
        self._token = token
        self._event = event or GithubEvent.from_env()
        logger.debug(f"Github event: {json.dumps(self._event.payload)}")
        self._pr_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_with_gaps_builder: t.Optional[PlanBuilder] = None
        self._check_run_mapping: t.Dict[str, CheckRun] = {}
        self._console = MarkdownConsole(console=Console(no_color=True))
        self._client: Github = client or Github(
            base_url=os.environ["GITHUB_API_URL"],
            login_or_token=self._token,
        )
        self._repo: Repository = self._client.get_repo(
            self._event.pull_request_info.full_repo_path, lazy=True
        )
        self._pull_request: PullRequest = self._repo.get_pull(
            self._event.pull_request_info.pr_number
        )
        self._issue: Issue = self._repo.get_issue(self._event.pull_request_info.pr_number)
        self._reviews: t.Iterable[PullRequestReview] = self._pull_request.get_reviews()
        # TODO: The python module says that user names can be None and this is not currently handled
        self._approvers: t.Set[str] = {
            review.user.login or "UNKNOWN"
            for review in self._reviews
            if review.state.lower() == "approved"
        }
        logger.debug(f"Approvers: {', '.join(self._approvers)}")
        self._context: Context = Context(
            paths=self._paths,
            config=self.config,
            console=self._console,
        )

    @property
    def deploy_command_enabled(self) -> bool:
        return self.bot_config.enable_deploy_command

    @property
    def is_comment_added(self) -> bool:
        return self._event.is_comment_added

    @property
    def _required_approvers(self) -> t.List[User]:
        required_approvers = [
            user
            for user in self._context.users
            if user.is_required_approver and user.github_username
        ]
        logger.debug(
            f"Required approvers: {', '.join(user.github_username for user in required_approvers if user.github_username)}"
        )
        return required_approvers

    @property
    def _required_approvers_with_approval(self) -> t.List[User]:
        return [
            user for user in self._required_approvers if user.github_username in self._approvers
        ]

    @property
    def pr_environment_name(self) -> str:
        return Environment.sanitize_name(
            "_".join(
                [
                    self.bot_config.pr_environment_name or self._event.pull_request_info.repo,
                    str(self._event.pull_request_info.pr_number),
                ]
            )
        )

    @property
    def do_required_approval_check(self) -> bool:
        """We want to skip required approval check if no users have this role"""
        do_required_approval_check = bool(self._required_approvers)
        logger.debug(f"Do required approval check: {do_required_approval_check}")
        return do_required_approval_check

    @property
    def has_required_approval(self) -> bool:
        """
        Check if the PR has a required approver.

        TODO: Allow defining requiring some number, or all, required approvers.
        """
        if not self._required_approvers or self._required_approvers_with_approval:
            logger.debug("Has required Approval")
            return True
        logger.debug("Does not have required approval")
        return False

    @property
    def pr_plan(self) -> Plan:
        if not self._pr_plan_builder:
            self._pr_plan_builder = self._context.plan_builder(
                environment=self.pr_environment_name,
                skip_tests=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                start=self.bot_config.default_pr_start,
                skip_backfill=self.bot_config.skip_pr_backfill,
                include_unmodified=self.bot_config.pr_include_unmodified,
            )
        assert self._pr_plan_builder
        return self._pr_plan_builder.build()

    @property
    def prod_plan(self) -> Plan:
        if not self._prod_plan_builder:
            self._prod_plan_builder = self._context.plan_builder(
                c.PROD,
                no_gaps=True,
                skip_tests=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                run=self.bot_config.run_on_deploy_to_prod,
            )
        assert self._prod_plan_builder
        return self._prod_plan_builder.build()

    @property
    def prod_plan_with_gaps(self) -> Plan:
        if not self._prod_plan_with_gaps_builder:
            self._prod_plan_with_gaps_builder = self._context.plan_builder(
                c.PROD,
                no_gaps=False,
                no_auto_categorization=True,
                skip_tests=True,
                run=self.bot_config.run_on_deploy_to_prod,
            )
        assert self._prod_plan_with_gaps_builder
        return self._prod_plan_with_gaps_builder.build()

    @property
    def bot_config(self) -> GithubCICDBotConfig:
        bot_config = self._context.config.cicd_bot or GithubCICDBotConfig(
            auto_categorize_changes=self._context.auto_categorize_changes
        )
        logger.debug(f"Bot config: {bot_config.json(indent=2)}")
        return bot_config

    @property
    def modified_snapshots(self) -> t.Dict[SnapshotId, t.Union[Snapshot, SnapshotTableInfo]]:
        return self.prod_plan_with_gaps.modified_snapshots

    @property
    def removed_snapshots(self) -> t.Set[SnapshotId]:
        return set(self.prod_plan_with_gaps.context_diff.removed_snapshots)

    @classmethod
    def _append_output(cls, key: str, value: str) -> None:
        """
        Appends the given key/value to output so they can be read by following steps
        """
        logger.debug(f"Setting output. Key: {key}, Value: {value}")
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
            print(f"{key}={value}", file=fh)

    def get_plan_summary(self, plan: Plan) -> str:
        try:
            # Clear out any output that might exist from prior steps
            self._console.clear_captured_outputs()
            self._console.show_model_difference_summary(
                context_diff=plan.context_diff,
                environment_naming_info=plan.environment_naming_info,
                default_catalog=self._context.default_catalog,
                no_diff=False,
            )
            difference_summary = self._console.consume_captured_output()
            self._console._show_missing_dates(plan, self._context.default_catalog)
            missing_dates = self._console.consume_captured_output()
            if not difference_summary and not missing_dates:
                return "No changes to apply."
            return f"{difference_summary}\n{missing_dates}"
        except PlanError as e:
            return f"Plan failed to generate. Check for pending or unresolved changes. Error: {e}"

    def run_tests(self) -> t.Tuple[unittest.result.TestResult, str]:
        """
        Run tests for the PR
        """
        return self._context._run_tests(verbose=True)

    def _get_or_create_comment(self, header: str = BOT_HEADER_MSG) -> IssueComment:
        comment = seq_get(
            [comment for comment in self._issue.get_comments() if header in comment.body],
            0,
        )
        if not comment:
            logger.debug(f"Did not find comment so creating one with header: {header}")
            return self._issue.create_comment(header)
        logger.debug(f"Found comment with header: {header}")
        return comment

    def _get_merge_state_status(self) -> MergeStateStatus:
        """
        This feature is currently in preview and therefore not available in the python module.
        So we query GraphQL directly instead.
        """
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github.merge-info-preview+json",
        }
        query = f"""{{
            repository(owner: "{self._event.pull_request_info.owner}", name: "{self._event.pull_request_info.repo}") {{
                pullRequest(number: {self._event.pull_request_info.pr_number}) {{
                    title
                    state
                    mergeStateStatus
                }}
            }}
        }}"""
        request = requests.post(
            os.environ["GITHUB_GRAPHQL_URL"],
            json={"query": query},
            headers=headers,
        )
        if request.status_code == 200:
            merge_status = MergeStateStatus(
                request.json()["data"]["repository"]["pullRequest"]["mergeStateStatus"].lower()
            )
            logger.debug(f"Merge state status: {merge_status.value}")
            return merge_status
        raise CICDBotError(f"Unable to get merge state status. Error: {request.text}")

    def update_sqlmesh_comment_info(
        self, value: str, *, dedup_regex: t.Optional[str]
    ) -> t.Tuple[bool, IssueComment]:
        """
        Update the SQLMesh PR Comment for the given lookup key with the given value. If a comment does not exist then
        it creates one. It determines the comment to update by looking for a comment with the header. If a dedup
        regex is provided then it will check if the value already exists in the comment and if so it will not update
        """
        comment = self._get_or_create_comment()
        if dedup_regex:
            # If we find a match against the regex then we just return since the comment has already been posted
            if seq_get(re.findall(dedup_regex, comment.body), 0):
                return False, comment
        full_comment = f"{comment.body}\n{value}"
        body, *truncated = self._chunk_up_api_message(f"{full_comment}")
        if truncated:
            logger.warning(
                f"Comment body was too long so we truncated it. Full text: {full_comment}"
            )
        comment.edit(body=body)
        return True, comment

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
        # If the PR is already merged then we will not deploy to prod if this event was triggered prior to the merge.
        # The deploy can still happen if the workflow is configured to listen for `closed` events.
        if self._pull_request.merged and not self._event.is_pull_request_closed:
            raise CICDBotError(
                "PR is already merged and this event was triggered prior to the merge."
            )
        merge_status = self._get_merge_state_status()
        if merge_status.is_dirty:
            raise CICDBotError(
                "Merge commit cannot be cleanly created. Likely from a merge conflict. "
                "Please check PR and resolve any issues."
            )
        plan_summary = f"""<details>
  <summary>:ship: Prod Plan Being Applied</summary>

{self.get_plan_summary(self.prod_plan)}
</details>

"""
        self.update_sqlmesh_comment_info(
            value=plan_summary,
            dedup_regex=None,
        )
        self._context.apply(self.prod_plan)

    def try_invalidate_pr_environment(self) -> None:
        """
        Marks the PR environment for garbage collection.
        """
        if self.bot_config.invalidate_environment_after_deploy:
            self._context.invalidate_environment(self.pr_environment_name)

    def _update_check(
        self,
        name: str,
        status: GithubCheckStatus,
        title: str,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        full_summary: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of the merge commit.
        """
        current_time = now()
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
        full_summary = full_summary or title
        summary, text, *truncated = self._chunk_up_api_message(full_summary) + [None]
        if truncated and truncated[0] is not None:
            logger.warning(f"Summary was too long so we truncated it. Full text: {full_summary}")
        kwargs["output"] = {"title": title, "summary": summary}
        if text:
            kwargs["output"]["text"] = text
        logger.debug(f"Updating check with kwargs: {kwargs}")
        if name in self._check_run_mapping:
            logger.debug(f"Found check run in mapping so updating it. Name: {name}")
            check_run = self._check_run_mapping[name]
            check_run.edit(
                **{k: v for k, v in kwargs.items() if k not in ("name", "head_sha", "started_at")}
            )
        else:
            logger.debug(f"Did not find check run in mapping so creating it. Name: {name}")
            self._check_run_mapping[name] = self._repo.create_check_run(**kwargs)
        if conclusion:
            self._append_output(
                word_characters_only(name.replace("SQLMesh - ", "").lower()), conclusion.value
            )

    def _update_check_handler(
        self,
        check_name: str,
        status: GithubCheckStatus,
        conclusion: t.Optional[GithubCheckConclusion],
        status_handler: t.Callable[[GithubCheckStatus], t.Tuple[str, t.Optional[str]]],
        conclusion_handler: t.Callable[
            [GithubCheckConclusion], t.Tuple[GithubCheckConclusion, str, t.Optional[str]]
        ],
    ) -> None:
        if conclusion:
            conclusion, title, summary = conclusion_handler(conclusion)
        else:
            title, summary = status_handler(status)
        self._update_check(
            name=check_name,
            status=status,
            title=title,
            conclusion=conclusion,
            full_summary=summary,
        )

    def update_test_check(
        self,
        status: GithubCheckStatus,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        result: t.Optional[unittest.result.TestResult] = None,
        output: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of tests for code in the PR
        """

        def conclusion_handler(
            conclusion: GithubCheckConclusion,
            result: t.Optional[unittest.result.TestResult],
            output: t.Optional[str],
        ) -> t.Tuple[GithubCheckConclusion, str, t.Optional[str]]:
            if result:
                # Clear out console
                self._console.consume_captured_output()
                self._console.log_test_results(
                    result, output, self._context._test_connection_config._engine_adapter.DIALECT
                )
                test_summary = self._console.consume_captured_output()
                test_title = "Tests Passed" if result.wasSuccessful() else "Tests Failed"
                test_conclusion = (
                    GithubCheckConclusion.SUCCESS
                    if result.wasSuccessful()
                    else GithubCheckConclusion.FAILURE
                )
                return test_conclusion, test_title, test_summary
            test_title = "Skipped Tests" if conclusion.is_skipped else "Tests Failed"
            return conclusion, test_title, output

        self._update_check_handler(
            check_name="SQLMesh - Run Unit Tests",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GithubCheckStatus.IN_PROGRESS: "Running Tests",
                    GithubCheckStatus.QUEUED: "Waiting to Run Tests",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, result=result, output=output),
        )

    def update_required_approval_check(
        self, status: GithubCheckStatus, conclusion: t.Optional[GithubCheckConclusion] = None
    ) -> None:
        """
        Updates the status of the merge commit for the required approval.
        """

        def conclusion_handler(
            conclusion: GithubCheckConclusion,
        ) -> t.Tuple[GithubCheckConclusion, str, t.Optional[str]]:
            test_summary = "**List of possible required approvers:**\n"
            for user in self._required_approvers:
                test_summary += f"- `{user.github_username or user.username}`\n"

            title = (
                f"Obtained approval from required approvers: {', '.join([user.github_username or user.username for user in self._required_approvers_with_approval])}"
                if conclusion.is_success
                else "Need a Required Approval"
            )
            return conclusion, title, test_summary

        # If we get a skip that means required approvers is not configured therefore it does not need to be displayed
        if conclusion and conclusion.is_skipped:
            return

        self._update_check_handler(
            check_name="SQLMesh - Has Required Approval",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GithubCheckStatus.IN_PROGRESS: "Checking if we have required Approvers",
                    GithubCheckStatus.QUEUED: "Waiting to Check if we have required Approvers",
                }[status],
                None,
            ),
            conclusion_handler=conclusion_handler,
        )

    def update_pr_environment_check(
        self,
        status: GithubCheckStatus,
        exception: t.Optional[Exception] = None,
    ) -> t.Optional[GithubCheckConclusion]:
        """
        Updates the status of the merge commit for the PR environment.
        """
        conclusion: t.Optional[GithubCheckConclusion] = None
        if isinstance(exception, (NoChangesPlanError, TestFailure)):
            conclusion = GithubCheckConclusion.SKIPPED
        elif isinstance(exception, UncategorizedPlanError):
            conclusion = GithubCheckConclusion.ACTION_REQUIRED
        elif exception:
            conclusion = GithubCheckConclusion.FAILURE
        elif status.is_completed:
            conclusion = GithubCheckConclusion.SUCCESS

        check_title_static = "PR Virtual Data Environment: "
        check_title = check_title_static + self.pr_environment_name

        def conclusion_handler(
            conclusion: GithubCheckConclusion, exception: t.Optional[Exception]
        ) -> t.Tuple[GithubCheckConclusion, str, t.Optional[str]]:
            if conclusion.is_success:
                if not self.modified_snapshots:
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
                    for modified_snapshot in self.modified_snapshots.values():
                        # We don't want to display indirect non-breaking since to users these are effectively no-op changes
                        if modified_snapshot.is_indirect_non_breaking:
                            continue
                        if modified_snapshot.snapshot_id in self.removed_snapshots:
                            # This will be an FQN since we don't have access to node name from a snapshot table info
                            # which is what a removed snapshot is
                            model_name = modified_snapshot.name
                            change_category = SNAPSHOT_CHANGE_CATEGORY_STR[
                                SnapshotChangeCategory.BREAKING
                            ]
                            interval_output = "REMOVED"
                        else:
                            assert isinstance(modified_snapshot, Snapshot)
                            model_name = modified_snapshot.node.name
                            change_category = (
                                "Uncategorized"
                                if not modified_snapshot.change_category
                                else SNAPSHOT_CHANGE_CATEGORY_STR[modified_snapshot.change_category]
                            )
                            intervals = (
                                modified_snapshot.dev_intervals
                                if modified_snapshot.is_forward_only
                                else modified_snapshot.intervals
                            )
                            interval_output = (
                                format_intervals(intervals, modified_snapshot.node.interval_unit)
                                if intervals
                                else "N/A"
                            )
                        body_rows.append(
                            [
                                h("td", model_name, autoescape=False),
                                h("td", change_category),
                                h("td", interval_output),
                            ]
                        )
                    table_header = h("thead", [h("tr", row) for row in header_rows])
                    table_body = h("tbody", [h("tr", row) for row in body_rows])
                    summary = str(h("table", [table_header, table_body]))
                vde_title = (
                    "- :eyes: To **review** this PR's changes, use virtual data environment:"
                )
                comment_value = f"{vde_title}\n  - `{self.pr_environment_name}`"
                if self.bot_config.enable_deploy_command:
                    comment_value += "\n- :arrow_forward: To **apply** this PR's plan to prod, comment:\n  - `/deploy`"
                dedup_regex = vde_title.replace("*", r"\*") + r".*"
                updated_comment, _ = self.update_sqlmesh_comment_info(
                    value=comment_value,
                    dedup_regex=dedup_regex,
                )
                if updated_comment:
                    self._append_output("created_pr_environment", "true")
            else:
                if isinstance(exception, NoChangesPlanError):
                    skip_reason = "No changes were detected compared to the prod environment."
                elif isinstance(exception, TestFailure):
                    skip_reason = "Unit Test(s) Failed so skipping PR creation"
                else:
                    skip_reason = "A prior stage failed resulting in skipping PR creation."

                captured_errors = self._console.consume_captured_errors()
                if captured_errors:
                    logger.debug(f"Captured errors: {captured_errors}")
                    failure_msg = f"**Errors:**\n{captured_errors}\n"
                elif isinstance(exception, NodeExecutionFailedError):
                    logger.debug(
                        "Got Node Execution Failed Error. Stack trace: " + traceback.format_exc()
                    )
                    failure_msg = f"Node `{exception.node.name}` failed to apply.\n\n**Stack Trace:**\n```\n{traceback.format_exc()}\n```"
                else:
                    logger.debug(
                        "Got unexpected error. Error Type: "
                        + str(type(exception))
                        + " Stack trace: "
                        + traceback.format_exc()
                    )
                    failure_msg = f"This is an unexpected error.\n\n**Exception:**\n```\n{traceback.format_exc()}\n```"
                conclusion_to_summary = {
                    GithubCheckConclusion.SKIPPED: f":next_track_button: Skipped creating or updating PR Environment `{self.pr_environment_name}`. {skip_reason}",
                    GithubCheckConclusion.FAILURE: f":x: Failed to create or update PR Environment `{self.pr_environment_name}`.\n{failure_msg}",
                    GithubCheckConclusion.CANCELLED: f":stop_sign: Cancelled creating or updating PR Environment `{self.pr_environment_name}`",
                    GithubCheckConclusion.ACTION_REQUIRED: f":warning: Action Required to create or update PR Environment `{self.pr_environment_name}`. There are likely uncateogrized changes. Run `plan` locally to apply these changes. If you want the bot to automatically categorize changes, then check documentation (https://sqlmesh.readthedocs.io/en/stable/integrations/github/) for more information.",
                }
                summary = conclusion_to_summary.get(
                    conclusion, f":interrobang: Got an unexpected conclusion: {conclusion.value}"
                )
            self._append_output("pr_environment_name", self.pr_environment_name)
            return conclusion, check_title, summary

        self._update_check_handler(
            check_name="SQLMesh - PR Environment Synced",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                check_title,
                {
                    GithubCheckStatus.QUEUED: f":pause_button: Waiting to create or update PR Environment `{self.pr_environment_name}`",
                    GithubCheckStatus.IN_PROGRESS: f":rocket: Creating or Updating PR Environment `{self.pr_environment_name}`",
                }[status],
            ),
            conclusion_handler=functools.partial(conclusion_handler, exception=exception),
        )
        return conclusion

    def update_prod_plan_preview_check(
        self,
        status: GithubCheckStatus,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        summary: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of the merge commit for the prod plan preview.
        """

        def conclusion_handler(
            conclusion: GithubCheckConclusion, summary: t.Optional[str] = None
        ) -> t.Tuple[GithubCheckConclusion, str, t.Optional[str]]:
            conclusion_to_title = {
                GithubCheckConclusion.SUCCESS: "Prod Plan Preview",
                GithubCheckConclusion.CANCELLED: "Cancelled generating prod plan preview",
                GithubCheckConclusion.SKIPPED: "Skipped generating prod plan preview since PR was not synchronized",
                GithubCheckConclusion.FAILURE: "Failed to generate prod plan preview",
            }
            title = conclusion_to_title.get(
                conclusion, f"Got an unexpected conclusion: {conclusion.value}"
            )
            return conclusion, title, summary

        self._update_check_handler(
            check_name="SQLMesh - Prod Plan Preview",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GithubCheckStatus.IN_PROGRESS: "Generating Prod Plan",
                    GithubCheckStatus.QUEUED: "Waiting to Generate Prod Plan",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, summary=summary),
        )

    def update_prod_environment_check(
        self,
        status: GithubCheckStatus,
        conclusion: t.Optional[GithubCheckConclusion] = None,
        skip_reason: t.Optional[str] = None,
    ) -> None:
        """
        Updates the status of the merge commit for the prod environment.
        """

        def conclusion_handler(
            conclusion: GithubCheckConclusion, skip_reason: t.Optional[str] = None
        ) -> t.Tuple[GithubCheckConclusion, str, t.Optional[str]]:
            conclusion_to_title = {
                GithubCheckConclusion.SUCCESS: "Deployed to Prod",
                GithubCheckConclusion.CANCELLED: "Cancelled deploying to prod",
                GithubCheckConclusion.SKIPPED: skip_reason,
                GithubCheckConclusion.FAILURE: "Failed to deploy to prod",
            }
            title = (
                conclusion_to_title.get(conclusion)
                or f"Got an unexpected conclusion: {conclusion.value}"
            )
            if conclusion.is_skipped:
                summary = title
            elif conclusion.is_failure:
                captured_errors = self._console.consume_captured_errors()
                summary = (
                    captured_errors or f"{title}\n\n**Error:**\n```\n{traceback.format_exc()}\n```"
                )
            else:
                summary = "**Generated Prod Plan**\n" + self.get_plan_summary(self.prod_plan)
            return conclusion, title, summary

        self._update_check_handler(
            check_name="SQLMesh - Prod Environment Synced",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GithubCheckStatus.IN_PROGRESS: "Deploying to Prod",
                    GithubCheckStatus.QUEUED: "Waiting to see if we can deploy to prod",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, skip_reason=skip_reason),
        )

    def try_merge_pr(self) -> None:
        """
        Merges the PR using the merge method defined in the bot config. If one is not defined then a merge is not
        performed
        """
        if self.bot_config.merge_method:
            logger.debug(f"Merging PR with merge method: {self.bot_config.merge_method.value}")
            self._pull_request.merge(merge_method=self.bot_config.merge_method.value)
        else:
            logger.debug("No merge method defined so skipping merge")

    def get_command_from_comment(self) -> BotCommand:
        """
        Gets the command from the comment
        """
        if not self._event.is_comment_added:
            logger.debug("Event is not a comment so returning invalid")
            return BotCommand.INVALID
        if self._event.pull_request_comment_body is None:
            raise CICDBotError("Unable to get comment body")
        logger.debug(f"Getting command from comment body: {self._event.pull_request_comment_body}")
        return BotCommand.from_comment_body(
            self._event.pull_request_comment_body, self.bot_config.command_namespace
        )

    def _chunk_up_api_message(self, message: str) -> t.List[str]:
        """
        Chunks up the message into `MAX_BYTE_LENGTH` byte chunks
        """
        message_encoded = message.encode("utf-8")
        return [
            message_encoded[i : i + self.MAX_BYTE_LENGTH].decode("utf-8", "ignore")
            for i in range(0, len(message_encoded), self.MAX_BYTE_LENGTH)
        ]
