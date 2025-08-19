from __future__ import annotations

import functools
import json
import logging
import os
import traceback
import typing as t
from enum import Enum
from pathlib import Path

from sqlglot.errors import SqlglotError

from sqlmesh.core import constants as c
from sqlmesh.core.console import get_console, MarkdownConsole
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.core.plan import Plan, PlanBuilder
from sqlmesh.core.test.result import ModelTextTestResult
from sqlmesh.core.user import User
from sqlmesh.core.config import Config
from sqlmesh.integrations.gitlab.cicd.config import GitlabCICDBotConfig
from sqlmesh.utils import Verbosity
from sqlmesh.utils.errors import (
    CICDBotError,
    NoChangesPlanError,
    PlanError,
    UncategorizedPlanError,
    LinterError,
    SQLMeshError,
)

if t.TYPE_CHECKING:
    from gitlab import Gitlab
    from gitlab.v4.objects import Project, ProjectMergeRequest, ProjectMergeRequestNote
    from sqlmesh.core.plan.definition import UserProvidedFlags

logger = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class GitlabEvent:
    def __init__(self, payload: t.Dict[str, t.Any]) -> None:
        self.payload = payload

    @classmethod
    def from_env(cls) -> GitlabEvent:
        if os.environ.get("GITLAB_EVENT_PATH"):
            with open(os.environ["GITLAB_EVENT_PATH"], "r", encoding="utf-8") as f:
                return cls(json.load(f))
        return cls({})

    @property
    def is_comment_added(self) -> bool:
        return (
            self.payload.get("object_kind") == "note" and self.payload.get("event_type") == "note"
        )

    @property
    def comment_body(self) -> t.Optional[str]:
        if self.is_comment_added:
            return self.payload.get("object_attributes", {}).get("note")
        return None


class GitlabCommitStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def is_success(self) -> bool:
        return self == GitlabCommitStatus.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self == GitlabCommitStatus.FAILED

    @property
    def is_skipped(self) -> bool:
        return self == GitlabCommitStatus.CANCELED


class BotCommand(Enum):
    INVALID = 1
    DEPLOY_PROD = 2

    @classmethod
    def from_comment_body(cls, body: str) -> BotCommand:
        body = body.strip()
        if body == "/deploy":
            return cls.DEPLOY_PROD
        return cls.INVALID

    @property
    def is_invalid(self) -> bool:
        return self == self.INVALID

    @property
    def is_deploy_prod(self) -> bool:
        return self == self.DEPLOY_PROD


class GitlabController:
    BOT_HEADER_MSG = ":robot: **SQLMesh Bot Info** :robot:"

    def __init__(
        self,
        paths: t.Union[Path, t.Iterable[Path]],
        token: str,
        config: t.Optional[t.Union[Config, str]] = None,
        event: t.Optional[GitlabEvent] = None,
        client: t.Optional[Gitlab] = None,
        context: t.Optional[Context] = None,
    ) -> None:
        from gitlab import Gitlab

        logger.debug(f"Initializing GitlabController with paths: {paths} and config: {config}")

        self.config = config
        self._paths = paths
        self._token = token
        self._event = event or GitlabEvent.from_env()
        logger.debug(f"Gitlab event: {json.dumps(self._event.payload)}")
        self._pr_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_builder: t.Optional[PlanBuilder] = None
        self._prod_plan_with_gaps_builder: t.Optional[PlanBuilder] = None

        if not isinstance(get_console(), MarkdownConsole):
            raise CICDBotError("Console must be a markdown console.")
        self._console = t.cast(MarkdownConsole, get_console())

        self._client: Gitlab = client or Gitlab(private_token=self._token)

        self._context: Context = context or Context(paths=self._paths, config=self.config)
        bot_config = self._context.config.cicd_bot or GitlabCICDBotConfig()
        if not isinstance(bot_config, GitlabCICDBotConfig):
            raise ValueError("CICD bot config must be of type GitlabCICDBotConfig")
        self.bot_config: GitlabCICDBotConfig = bot_config

        project_id = self.bot_config.project_id or os.environ.get("CI_PROJECT_ID")
        if not project_id:
            raise ValueError("GitLab project ID is required.")
        self._project: Project = self._client.projects.get(project_id)

        mr_iid = os.environ.get("CI_MERGE_REQUEST_IID")
        if not mr_iid:
            raise ValueError("GitLab merge request IID is required.")
        self._merge_request: ProjectMergeRequest = self._project.mergerequests.get(mr_iid)

    @property
    def deploy_command_enabled(self) -> bool:
        return self.bot_config.enable_deploy_command

    @property
    def is_comment_added(self) -> bool:
        return self._event.is_comment_added

    @property
    def _required_approvers(self) -> t.List[User]:
        # GitLab doesn't have a direct equivalent of GitHub's "required approvers" that can be easily checked via the API.
        # This would need to be implemented based on GitLab's approval rules, which can be more complex.
        # For now, we will return an empty list.
        return []

    @property
    def has_required_approval(self) -> bool:
        # See comment in `_required_approvers`
        return True

    @property
    def pr_environment_name(self) -> str:
        return Environment.sanitize_name(
            "_".join(
                [
                    self.bot_config.pr_environment_name or self._project.path,
                    str(self._merge_request.iid),
                ]
            )
        )

    @property
    def pr_targets_prod_branch(self) -> bool:
        return self._merge_request.target_branch in self.bot_config.prod_branch_names

    @property
    def forward_only_plan(self) -> bool:
        default = self._context.config.plan.forward_only
        source_branch = self._merge_request.source_branch
        if isinstance(source_branch, str):
            return source_branch.endswith(self.bot_config.forward_only_branch_suffix) or default
        return default

    @property
    def pr_plan(self) -> Plan:
        if not self._pr_plan_builder:
            self._pr_plan_builder = self._context.plan_builder(
                environment=self.pr_environment_name,
                skip_tests=True,
                skip_linter=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                start=self.bot_config.default_pr_start,
                min_intervals=self.bot_config.pr_min_intervals,
                skip_backfill=self.bot_config.skip_pr_backfill,
                include_unmodified=self.bot_config.pr_include_unmodified,
                forward_only=self.forward_only_plan,
            )
        assert self._pr_plan_builder
        return self._pr_plan_builder.build()

    @property
    def pr_plan_or_none(self) -> t.Optional[Plan]:
        try:
            return self.pr_plan
        except:
            return None

    @property
    def pr_plan_flags(self) -> t.Optional[t.Dict[str, UserProvidedFlags]]:
        if pr_plan := self.pr_plan_or_none:
            return pr_plan.user_provided_flags
        if pr_plan_builder := self._pr_plan_builder:
            return pr_plan_builder._user_provided_flags
        return None

    @property
    def prod_plan(self) -> Plan:
        if not self._prod_plan_builder:
            self._prod_plan_builder = self._context.plan_builder(
                c.PROD,
                no_gaps=True,
                skip_tests=True,
                skip_linter=True,
                categorizer_config=self.bot_config.auto_categorize_changes,
                run=self.bot_config.run_on_deploy_to_prod,
                forward_only=self.forward_only_plan,
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
                skip_linter=True,
                run=self.bot_config.run_on_deploy_to_prod,
                forward_only=self.forward_only_plan,
            )
        assert self._prod_plan_with_gaps_builder
        return self._prod_plan_with_gaps_builder.build()

    def get_plan_summary(self, plan: Plan) -> str:
        orig_verbosity = self._console.verbosity
        self._console.verbosity = Verbosity.VERY_VERBOSE

        try:
            self._console.consume_captured_output()
            if plan.restatements:
                self._console._print("\n**Restating models**\n")
            else:
                self._console.show_environment_difference_summary(
                    context_diff=plan.context_diff,
                    no_diff=False,
                )
            if plan.context_diff.has_changes:
                self._console.show_model_difference_summary(
                    context_diff=plan.context_diff,
                    environment_naming_info=plan.environment_naming_info,
                    default_catalog=self._context.default_catalog,
                    no_diff=False,
                )
            difference_summary = self._console.consume_captured_output()
            self._console._show_missing_dates(plan, self._context.default_catalog)
            missing_dates = self._console.consume_captured_output()

            plan_flags_section = (
                f"\n\n{self._generate_plan_flags_section(plan.user_provided_flags)}"
                if plan.user_provided_flags
                else ""
            )

            if not difference_summary and not missing_dates:
                return f"No changes to apply.{plan_flags_section}"

            warnings_block = self._console.consume_captured_warnings()
            errors_block = self._console.consume_captured_errors()

            return f"{warnings_block}{errors_block}{difference_summary}\n{missing_dates}{plan_flags_section}"
        except PlanError as e:
            logger.exception("Plan failed to generate")
            return f"Plan failed to generate. Check for pending or unresolved changes. Error: {e}"
        finally:
            self._console.verbosity = orig_verbosity

    def run_tests(self) -> t.Tuple[ModelTextTestResult, str]:
        return self._context._run_tests(verbosity=Verbosity.VERBOSE)

    def run_linter(self) -> None:
        self._console.consume_captured_output()
        self._context.lint_models()

    def update_sqlmesh_comment_info(self, value: str) -> ProjectMergeRequestNote:
        body = f"{self.BOT_HEADER_MSG}\n{value}"
        return self._merge_request.notes.create({"body": body})

    def update_pr_environment(self) -> None:
        self._console.consume_captured_output()
        self._context.apply(self.pr_plan)

        vde_title = "- :eyes: To **review** this PR's changes, use virtual data environment:"
        comment_value = f"{vde_title}\n  - `{self.pr_environment_name}`"
        if self.bot_config.enable_deploy_command:
            comment_value += (
                f"\n- :arrow_forward: To **apply** this PR's plan to prod, comment:\n  - `/deploy`"
            )
        self.update_sqlmesh_comment_info(value=comment_value)

    def deploy_to_prod(self) -> None:
        if self._merge_request.state == "merged":
            raise CICDBotError("Merge request is already merged.")
        plan_summary = f"""<details>
<summary>:ship: Prod Plan Being Applied</summary>

{self.get_plan_summary(self.prod_plan)}
</details>
"""
        self.update_sqlmesh_comment_info(value=plan_summary)
        self._context.apply(self.prod_plan)

    def try_invalidate_pr_environment(self) -> None:
        if self.bot_config.invalidate_environment_after_deploy:
            self._context.invalidate_environment(self.pr_environment_name)

    def _update_check(
        self,
        name: str,
        status: GitlabCommitStatus,
        description: str,
        target_url: t.Optional[str] = None,
    ) -> None:
        logger.debug(f"Updating check '{name}' to '{status.value}'")
        self._project.commits.get(self._merge_request.sha).statuses.create(
            {
                "name": name,
                "status": status.value,
                "description": description,
                "target_url": target_url,
            }
        )

    def _update_check_handler(
        self,
        check_name: str,
        status: GitlabCommitStatus,
        conclusion: t.Optional[GitlabCommitStatus],
        status_handler: t.Callable[[GitlabCommitStatus], t.Tuple[str, t.Optional[str]]],
        conclusion_handler: t.Callable[
            [GitlabCommitStatus], t.Tuple[GitlabCommitStatus, str, t.Optional[str]]
        ],
    ) -> None:
        if conclusion:
            conclusion, description, target_url = conclusion_handler(conclusion)
        else:
            description, target_url = status_handler(status)
        self._update_check(
            name=check_name,
            status=conclusion or status,
            description=description,
            target_url=target_url,
        )

    def update_linter_check(
        self,
        status: GitlabCommitStatus,
        conclusion: t.Optional[GitlabCommitStatus] = None,
    ) -> None:
        if not self._context.config.linter.enabled:
            return

        def conclusion_handler(
            conclusion: GitlabCommitStatus,
        ) -> t.Tuple[GitlabCommitStatus, str, t.Optional[str]]:
            linter_summary = self._console.consume_captured_output() or "Linter Success"
            return conclusion, linter_summary, None

        self._update_check_handler(
            check_name="SQLMesh - Linter",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GitlabCommitStatus.RUNNING: "Running linter",
                    GitlabCommitStatus.PENDING: "Waiting to Run linter",
                }[status],
                None,
            ),
            conclusion_handler=conclusion_handler,
        )

    def update_test_check(
        self,
        status: GitlabCommitStatus,
        conclusion: t.Optional[GitlabCommitStatus] = None,
        result: t.Optional[ModelTextTestResult] = None,
        traceback: t.Optional[str] = None,
    ) -> None:
        def conclusion_handler(
            conclusion: GitlabCommitStatus,
            result: t.Optional[ModelTextTestResult],
        ) -> t.Tuple[GitlabCommitStatus, str, t.Optional[str]]:
            if result:
                self._console.consume_captured_output()
                self._console.log_test_results(
                    result,
                    self._context.test_connection_config._engine_adapter.DIALECT,
                )
                test_summary = self._console.consume_captured_output()
                test_conclusion = (
                    GitlabCommitStatus.SUCCESS
                    if result.wasSuccessful()
                    else GitlabCommitStatus.FAILED
                )
                return test_conclusion, test_summary, None
            if traceback:
                self._console._print(traceback)

            return conclusion, traceback or "Tests Failed", None

        self._update_check_handler(
            check_name="SQLMesh - Run Unit Tests",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GitlabCommitStatus.RUNNING: "Running Tests",
                    GitlabCommitStatus.PENDING: "Waiting to Run Tests",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, result=result),
        )

    def update_pr_environment_check(
        self, status: GitlabCommitStatus, exception: t.Optional[Exception] = None
    ) -> t.Optional[GitlabCommitStatus]:
        conclusion: t.Optional[GitlabCommitStatus] = None
        if isinstance(exception, (NoChangesPlanError, TestFailure, LinterError)):
            conclusion = GitlabCommitStatus.SUCCESS
        elif isinstance(exception, UncategorizedPlanError):
            conclusion = GitlabCommitStatus.FAILED
        elif exception:
            conclusion = GitlabCommitStatus.FAILED
        elif status == GitlabCommitStatus.SUCCESS:
            conclusion = GitlabCommitStatus.SUCCESS

        check_title = f"PR Virtual Data Environment: {self.pr_environment_name}"

        def conclusion_handler(
            conclusion: GitlabCommitStatus, exception: t.Optional[Exception]
        ) -> t.Tuple[GitlabCommitStatus, str, t.Optional[str]]:
            summary = self.get_pr_environment_summary(conclusion, exception)
            return conclusion, summary, None

        self._update_check_handler(
            check_name="SQLMesh - PR Environment Synced",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                check_title,
                {
                    GitlabCommitStatus.PENDING: f":pause_button: Waiting to create or update PR Environment `{self.pr_environment_name}`",
                    GitlabCommitStatus.RUNNING: f":rocket: Creating or Updating PR Environment `{self.pr_environment_name}`",
                }[status],
            ),
            conclusion_handler=functools.partial(conclusion_handler, exception=exception),
        )
        return conclusion

    def update_prod_plan_preview_check(
        self,
        status: GitlabCommitStatus,
        conclusion: t.Optional[GitlabCommitStatus] = None,
        summary: t.Optional[str] = None,
    ) -> None:
        def conclusion_handler(
            conclusion: GitlabCommitStatus, summary: t.Optional[str] = None
        ) -> t.Tuple[GitlabCommitStatus, str, t.Optional[str]]:
            title = "Prod Plan Preview"
            if conclusion == GitlabCommitStatus.SUCCESS and summary:
                summary = (
                    f"This is a preview that shows the differences between this PR environment `{self.pr_environment_name}` and `prod`.\n\n"
                    "These are the changes that would be deployed.\n\n"
                ) + summary
            return conclusion, title, summary

        self._update_check_handler(
            check_name="SQLMesh - Prod Plan Preview",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GitlabCommitStatus.RUNNING: "Generating Prod Plan",
                    GitlabCommitStatus.PENDING: "Waiting to Generate Prod Plan",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, summary=summary),
        )

    def update_prod_environment_check(
        self,
        status: GitlabCommitStatus,
        conclusion: t.Optional[GitlabCommitStatus] = None,
        skip_reason: t.Optional[str] = None,
        plan_error: t.Optional[PlanError] = None,
    ) -> None:
        def conclusion_handler(
            conclusion: GitlabCommitStatus, skip_reason: t.Optional[str] = None
        ) -> t.Tuple[GitlabCommitStatus, str, t.Optional[str]]:
            title = "Prod Environment Synced"
            if conclusion == GitlabCommitStatus.CANCELED:
                summary = skip_reason
            elif conclusion == GitlabCommitStatus.FAILED:
                captured_errors = self._console.consume_captured_errors()
                summary = (
                    captured_errors or f"{title}\n\n**Error:**\n```\n{traceback.format_exc()}\n```"
                )
            elif plan_error:
                summary = f"**Plan error:**\n```\n{plan_error}\n```"
            else:
                summary = "**Generated Prod Plan**\n" + self.get_plan_summary(self.prod_plan)
            return conclusion, title, summary

        self._update_check_handler(
            check_name="SQLMesh - Prod Environment Synced",
            status=status,
            conclusion=conclusion,
            status_handler=lambda status: (
                {
                    GitlabCommitStatus.RUNNING: "Deploying to Prod",
                    GitlabCommitStatus.PENDING: "Waiting to see if we can deploy to prod",
                }[status],
                None,
            ),
            conclusion_handler=functools.partial(conclusion_handler, skip_reason=skip_reason),
        )

    def try_merge_pr(self) -> None:
        logger.debug("Merging MR")
        self._merge_request.merge()

    def get_command_from_comment(self) -> BotCommand:
        if not self._event.is_comment_added:
            logger.debug("Event is not a comment so returning invalid")
            return BotCommand.INVALID
        if self._event.comment_body is None:
            raise CICDBotError("Unable to get comment body")
        logger.debug(f"Getting command from comment body: {self._event.comment_body}")
        return BotCommand.from_comment_body(self._event.comment_body)

    def _generate_plan_flags_section(
        self,
        user_provided_flags: t.Dict[str, UserProvidedFlags],
    ) -> str:
        section = "<details>\n\n<summary>Plan flags</summary>\n\n"
        for flag_name, flag_value in user_provided_flags.items():
            section += f"- `{flag_name}` = `{flag_value}`\n"
        section += "\n</details>"
        return section

    def get_pr_environment_summary(
        self,
        conclusion: GitlabCommitStatus,
        exception: t.Optional[Exception] = None,
    ) -> str:
        heading = ""
        summary = ""

        if conclusion == GitlabCommitStatus.SUCCESS:
            summary = self._get_pr_environment_summary_success()
        elif conclusion == GitlabCommitStatus.FAILED:
            heading = (
                f":x: Failed to create or update PR Environment `{self.pr_environment_name}` :x:"
            )
            summary = self._get_pr_environment_summary_failure(exception)
        else:
            heading = f":interrobang: Got an unexpected conclusion: {conclusion.value}"

        if warnings := self._console.consume_captured_warnings():
            summary = f"{warnings}\n{summary}"

        return f"{heading}\n\n{summary}".strip()

    def _get_pr_environment_summary_success(self) -> str:
        prod_plan = self.prod_plan_with_gaps
        if not prod_plan.has_changes:
            summary = "No models were modified in this PR.\n"
        else:
            summary = "Here is a summary of data that has been loaded into the PR environment and could be deployed to `prod`.\n\n"
        if prod_plan.user_provided_flags:
            summary += self._generate_plan_flags_section(prod_plan.user_provided_flags)
        return summary

    def _get_pr_environment_summary_failure(self, exception: t.Optional[Exception] = None) -> str:
        console_output = self._console.consume_captured_output()
        failure_msg = ""

        if isinstance(exception, PlanError):
            if exception.args and (msg := exception.args[0]) and isinstance(msg, str):
                failure_msg += f"*{msg}*\n"
            if console_output:
                failure_msg += f"\n{console_output}"
        elif isinstance(exception, (SQLMeshError, SqlglotError, ValueError)):
            failure_msg = f"**Error:** {str(exception)}"
        elif exception:
            logger.debug(
                "Got unexpected error. Error Type: "
                + str(type(exception))
                + " Stack trace: "
                + traceback.format_exc()
            )
            failure_msg = f"This is an unexpected error.\n\n**Exception:**\n```\n{traceback.format_exc()}\n```"

        if captured_errors := self._console.consume_captured_errors():
            failure_msg = f"{captured_errors}\n{failure_msg}"

        if plan_flags := self.pr_plan_flags:
            failure_msg += f"\n\n{self._generate_plan_flags_section(plan_flags)}"

        return failure_msg


def run_all_checks(controller: GitlabController) -> None:
    linter_error: t.Optional[LinterError] = None
    test_failure: t.Optional[TestFailure] = None
    uncategorized_error: t.Optional[UncategorizedPlanError] = None
    pr_env_creation_exception: t.Optional[Exception] = None
    pr_env_conclusion: t.Optional[GitlabCommitStatus] = None

    # Run Linter
    controller.update_linter_check(status=GitlabCommitStatus.RUNNING)
    try:
        controller.run_linter()
        controller.update_linter_check(
            status=GitlabCommitStatus.SUCCESS, conclusion=GitlabCommitStatus.SUCCESS
        )
    except LinterError as e:
        linter_error = e
        controller.update_linter_check(
            status=GitlabCommitStatus.FAILED, conclusion=GitlabCommitStatus.FAILED
        )
    except Exception:
        controller.update_linter_check(
            status=GitlabCommitStatus.FAILED, conclusion=GitlabCommitStatus.FAILED
        )
        raise

    # Run Tests
    controller.update_test_check(status=GitlabCommitStatus.RUNNING)
    try:
        if linter_error:
            raise linter_error
        result, traceback = controller.run_tests()
        if not result.wasSuccessful():
            test_failure = TestFailure()
        controller.update_test_check(
            status=GitlabCommitStatus.SUCCESS,
            result=result,
            traceback=traceback,
        )
    except LinterError:
        controller.update_test_check(
            status=GitlabCommitStatus.CANCELED, conclusion=GitlabCommitStatus.CANCELED
        )
    except Exception:
        controller.update_test_check(
            status=GitlabCommitStatus.FAILED, conclusion=GitlabCommitStatus.FAILED
        )
        raise

    # Create PR Environment
    try:
        if test_failure:
            raise test_failure
        pr_env_conclusion = controller.update_pr_environment_check(
            status=GitlabCommitStatus.RUNNING
        )
        controller.update_pr_environment()
        pr_env_conclusion = controller.update_pr_environment_check(
            status=GitlabCommitStatus.SUCCESS
        )
    except (NoChangesPlanError, TestFailure, LinterError) as e:
        pr_env_creation_exception = e
        pr_env_conclusion = controller.update_pr_environment_check(
            status=GitlabCommitStatus.SUCCESS, exception=e
        )
    except UncategorizedPlanError as e:
        uncategorized_error = e
        pr_env_creation_exception = e
        pr_env_conclusion = controller.update_pr_environment_check(
            status=GitlabCommitStatus.FAILED, exception=e
        )
    except Exception as e:
        pr_env_creation_exception = e
        pr_env_conclusion = controller.update_pr_environment_check(
            status=GitlabCommitStatus.FAILED, exception=e
        )
        raise
    finally:
        # Prod Plan Preview
        controller.update_prod_plan_preview_check(status=GitlabCommitStatus.RUNNING)
        if (
            pr_env_conclusion
            and pr_env_conclusion == GitlabCommitStatus.SUCCESS
            and controller.pr_targets_prod_branch
        ):
            try:
                summary = controller.get_plan_summary(controller.prod_plan_with_gaps)
                controller.update_prod_plan_preview_check(
                    status=GitlabCommitStatus.SUCCESS,
                    conclusion=GitlabCommitStatus.SUCCESS,
                    summary=summary,
                )
            except Exception:
                controller.update_prod_plan_preview_check(
                    status=GitlabCommitStatus.FAILED,
                    conclusion=GitlabCommitStatus.FAILED,
                )
                raise
        else:
            controller.update_prod_plan_preview_check(
                status=GitlabCommitStatus.CANCELED,
                conclusion=GitlabCommitStatus.CANCELED,
            )

        # Prod Environment
        controller.update_prod_environment_check(status=GitlabCommitStatus.RUNNING)
        if uncategorized_error:
            controller.update_prod_environment_check(
                status=GitlabCommitStatus.CANCELED,
                conclusion=GitlabCommitStatus.CANCELED,
                skip_reason="Skipped since there are uncategorized changes.",
            )
        elif pr_env_creation_exception:
            controller.update_prod_environment_check(
                status=GitlabCommitStatus.CANCELED,
                conclusion=GitlabCommitStatus.CANCELED,
                skip_reason="Skipped since the PR environment could not be created.",
            )
        elif not controller.pr_targets_prod_branch:
            controller.update_prod_environment_check(
                status=GitlabCommitStatus.CANCELED,
                conclusion=GitlabCommitStatus.CANCELED,
                skip_reason=f"Skipped since the MR does not target a prod branch. Current target is `{controller._merge_request.target_branch}` but needs to be one of `{controller.bot_config.prod_branch_names}`.",
            )
        elif controller.deploy_command_enabled:
            controller.update_prod_environment_check(
                status=GitlabCommitStatus.CANCELED,
                conclusion=GitlabCommitStatus.CANCELED,
                skip_reason="Skipped since an explicit command is required to deploy to prod.",
            )
        else:
            try:
                controller.deploy_to_prod()
                controller.update_prod_environment_check(
                    status=GitlabCommitStatus.SUCCESS,
                    conclusion=GitlabCommitStatus.SUCCESS,
                )
                controller.try_merge_pr()
                controller.try_invalidate_pr_environment()
            except PlanError as e:
                controller.update_prod_environment_check(
                    status=GitlabCommitStatus.FAILED,
                    conclusion=GitlabCommitStatus.FAILED,
                    plan_error=e,
                )
            except Exception:
                controller.update_prod_environment_check(
                    status=GitlabCommitStatus.FAILED,
                    conclusion=GitlabCommitStatus.FAILED,
                )
                raise


def run_deploy_command(controller: GitlabController) -> None:
    controller.update_prod_environment_check(status=GitlabCommitStatus.RUNNING)
    try:
        if not controller.has_required_approval:
            raise CICDBotError("Missing required approval.")
        controller.deploy_to_prod()
        controller.update_prod_environment_check(
            status=GitlabCommitStatus.SUCCESS,
            conclusion=GitlabCommitStatus.SUCCESS,
        )
        controller.try_merge_pr()
        controller.try_invalidate_pr_environment()
    except PlanError as e:
        controller.update_prod_environment_check(
            status=GitlabCommitStatus.FAILED,
            conclusion=GitlabCommitStatus.FAILED,
            plan_error=e,
        )
    except Exception:
        controller.update_prod_environment_check(
            status=GitlabCommitStatus.FAILED,
            conclusion=GitlabCommitStatus.FAILED,
        )
        raise
