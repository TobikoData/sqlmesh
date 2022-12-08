from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import typing as t
from enum import Enum

from sqlmesh import Context
from sqlmesh.core import constants as c
from sqlmesh.core.plan import Plan
from sqlmesh.integrations.github.models import PullRequestInfo
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.utils.errors import PlanError

if t.TYPE_CHECKING:
    from github import Github
    from github.Issue import Issue
    from github.PullRequest import PullRequest
    from github.PullRequestReview import PullRequestReview
    from github.Repository import Repository


class MissingARequiredApprover(Exception):
    pass


class InvalidConfigurationForBot(Exception):
    pass


class UnsupportedGithubAction(Exception):
    pass


class PlanRequiresUserInput(Exception):
    pass


class PlanRequiresBackfill(Exception):
    pass


class UnexpectedReviewState(Exception):
    pass


class UnknownComment(Exception):
    pass


PullRequestPayload = t.Dict[str, t.Any]


class GithubAction(str, Enum):
    UNKNOWN = "unknown"
    REVIEW = "review"
    COMMENT = "comment"
    PULL_REQUEST = "pull_request"

    @property
    def is_unknown(self) -> bool:
        return self == GithubAction.UNKNOWN

    @property
    def is_review(self) -> bool:
        return self == GithubAction.REVIEW

    @property
    def is_comment(self) -> bool:
        return self == GithubAction.COMMENT

    @property
    def is_pull_request(self) -> bool:
        return self == GithubAction.PULL_REQUEST


class Action(str, Enum):
    CHECK_GATEKEEPER = "check_gatekeeper"
    UPDATE_PR_ENVIRONMENT = "update_pr_environment"
    DEPLOY_PROD = "deploy_prod"
    GET_PULL_REQUEST_NUMBER = "get_pull_request_number"
    DELETE_ENVIRONMENT = "delete_environment"
    GET_MERGE_COMMIT_SHA = "get_merge_commit_sha"

    @property
    def is_check_gatekeeper(self) -> bool:
        return self == Action.CHECK_GATEKEEPER

    @property
    def is_update_pr_environment(self) -> bool:
        return self == Action.UPDATE_PR_ENVIRONMENT

    @property
    def is_deploy_prod(self) -> bool:
        return self == Action.DEPLOY_PROD

    @property
    def is_get_pull_request_number(self) -> bool:
        return self == Action.GET_PULL_REQUEST_NUMBER

    @property
    def is_delete_environment(self) -> bool:
        return self == Action.DELETE_ENVIRONMENT

    @property
    def is_get_merge_commit_sha(self) -> bool:
        return self == Action.GET_MERGE_COMMIT_SHA


class GithubPullRequestBot:
    def __init__(
        self,
        *,
        sqlmesh_root_dir: str,
        config_name: str,
        github_token: str,
        payload: PullRequestPayload,
        action: Action,
        github_base_url: t.Optional[str] = None,
    ) -> None:
        self.sqlmesh_root_dir = sqlmesh_root_dir
        self.config_name = config_name
        self.github_token = github_token
        self.github_base_url = github_base_url
        self.payload = payload
        self.action = action
        self._prod_plan: t.Optional[Plan] = None
        self._pr_plan: t.Optional[Plan] = None
        self._pull_request_info: t.Optional[PullRequestInfo] = None
        self._context: t.Optional[Context] = None
        self._github_client: t.Optional[Github] = None
        self._approvers: t.Set[str] = set()
        self._repo: t.Optional[Repository] = None
        self._pull_request: t.Optional[PullRequest] = None
        self._reviews: t.Optional[t.Iterable[PullRequestReview]] = None
        self._issue: t.Optional[Issue] = None

    @classmethod
    def create(
        cls,
        *,
        sqlmesh_root_dir: str,
        config_name: str,
        github_token: str,
        event_payload_path: pathlib.Path,
        github_base_url: t.Optional[str] = None,
        action: Action,
    ) -> GithubPullRequestBot:
        if not event_payload_path.is_file():
            raise InvalidConfigurationForBot(
                f"Provided path to event payload is not a file. Path: {str(event_payload_path)}"
            )
        payload = json.loads(event_payload_path.read_text())
        return cls(
            sqlmesh_root_dir=sqlmesh_root_dir,
            config_name=config_name,
            github_token=github_token,
            github_base_url=github_base_url,
            payload=payload,
            action=action,
        )

    @property
    def pull_request_url(self) -> str:
        if self.github_action.is_review:
            return self.payload["review"]["pull_request_url"]
        if self.github_action.is_comment:
            return self.payload["issue"]["pull_request"]["url"]
        if self.github_action.is_pull_request:
            return self.payload["pull_request"]["_links"]["self"]["href"]
        raise UnsupportedGithubAction(
            "Tried to get the pull request URL from an unknown action"
        )

    @property
    def context(self) -> Context:
        if not self._context:
            self._context = Context(
                path=self.sqlmesh_root_dir,
                config=self.config_name,
                notification_targets=[
                    GithubNotificationTarget(
                        token=self.github_token,
                        github_url=self.github_base_url,
                        pull_request_url=self.pull_request_url,
                    )
                ],
            )
        return self._context

    @property
    def github_client(self) -> Github:
        from github import Github

        if not self._github_client:
            self._github_client = (
                Github(login_or_token=self.github_token, base_url=self.github_base_url)
                if self.github_base_url
                else Github(login_or_token=self.github_token)
            )
        return self._github_client

    @property
    def pull_request_info(self) -> PullRequestInfo:
        if not self._pull_request_info:
            self._pull_request_info = PullRequestInfo.create_from_pull_request_url(
                self.pull_request_url
            )
        return self._pull_request_info

    @property
    def github_action(self) -> GithubAction:
        if self.payload.get("review"):
            return GithubAction.REVIEW
        if self.payload.get("comment"):
            return GithubAction.COMMENT
        if self.payload.get("pull_request"):
            return GithubAction.PULL_REQUEST
        return GithubAction.UNKNOWN

    @property
    def gatekeepers(self) -> t.Set[str]:
        return {
            user.github_login_username
            for user in self.context.config.users
            if user.github_login_username
        }

    @property
    def repo(self) -> Repository:
        if not self._repo:
            self._repo = self.github_client.get_repo(
                self.pull_request_info.full_repo_path, lazy=True
            )
        return self._repo

    @property
    def pull_request(self) -> PullRequest:
        if not self._pull_request:
            self._pull_request = self.repo.get_pull(
                number=self.pull_request_info.pr_number
            )
        return self._pull_request

    @property
    def reviews(self) -> t.Iterable[PullRequestReview]:
        if not self._reviews:
            self._reviews = self.pull_request.get_reviews()
        return self._reviews

    @property
    def issue(self) -> Issue:
        if not self._issue:
            self._issue = self.repo.get_issue(number=self.pull_request_info.pr_number)
        return self._issue

    @property
    def approvers(self) -> t.Set[str]:
        if not self._approvers:
            # TODO: It appears names can be None so I need to better understand why that can happen
            self._approvers = {
                review.user.name or "UNKNOWN"
                for review in self.reviews
                if review.state.lower() == "approved"
            }
        return self._approvers

    @property
    def prod_plan(self) -> Plan:
        if not self._prod_plan:
            self._prod_plan = self.context.plan(
                c.PROD, auto_apply_logical=True, skip_console=True
            )
        return self._prod_plan

    @property
    def pr_environment_name(self) -> str:
        return "_".join(
            [
                self.pull_request_info.repo,
                str(self.pull_request_info.pr_number),
            ]
        )

    @property
    def pr_plan(self) -> Plan:
        if not self._pr_plan:
            try:
                self._pr_plan = self.context.plan(
                    environment=self.pr_environment_name,
                    skip_backfill=True,
                    auto_apply_logical=True,
                    skip_console=True,
                )
            except PlanError as e:
                for notification_target in self.context.notification_targets:
                    notification_target.send(
                        f":heavy_exclamation_mark: PR has failing unit test(s) :heavy_exclamation_mark:"
                    )
                raise e
        return self._pr_plan

    @property
    def comment(self) -> str:
        return self.payload["comment"]["body"]

    def _clear_github_cache(self):
        self._repo = None
        self._pull_request = None
        self._reviews = None
        self._issue = None
        self._approvers = set()

    def _has_gatekeeper_approval(self) -> bool:
        if not self.gatekeepers:
            raise InvalidConfigurationForBot(
                "The bot was asked to check for gatekeepers but none are defined. Check your users in your config and make sure someone has the `is_gatekeeper` set to True."
            )
        return bool(
            [
                gatekeeper
                for gatekeeper in self.gatekeepers
                if gatekeeper in self.approvers
            ]
        )

    def _deploy_to_prod_and_merge(self) -> None:
        if self.prod_plan.missing_intervals:
            raise PlanRequiresUserInput()
        if self.prod_plan.context_diff.has_differences:
            self.context.apply(self.prod_plan, blocking=True)

    def _comment_on_pr(self, comment: str) -> None:
        self.issue.create_comment(comment)

    def _check_gatekeeper_approval(self):
        if not self._has_gatekeeper_approval():
            raise MissingARequiredApprover()
        return

    def process(self) -> None:
        requires_input_msg = f"{datetime.datetime.utcnow().isoformat(sep=' ', timespec='seconds')} - :heavy_exclamation_mark: PR contains changes that require user input. Run plan locally against your development environment to categorize. :heavy_exclamation_mark:"
        if self.action.is_check_gatekeeper:
            if not self._has_gatekeeper_approval():
                raise MissingARequiredApprover()
            return
        if self.action.is_update_pr_environment:
            if self.pr_plan.new_snapshots:
                for notification_target in self.context.notification_targets:
                    notification_target.send(requires_input_msg)
                # debug
                print(list(self.pr_plan.snapshots)[0].model.python_env)
                raise PlanRequiresUserInput(
                    f"New Snapshots: {self.pr_plan.new_snapshots}"
                )
            if self.pr_plan.context_diff.has_differences:
                self.context.apply(self.pr_plan, blocking=True)
            return
        if self.action.is_deploy_prod:
            if self.prod_plan.new_snapshots:
                for notification_target in self.context.notification_targets:
                    notification_target.send(requires_input_msg)
                raise PlanRequiresUserInput()
            elif self.prod_plan.missing_intervals:
                self.context.apply(self.prod_plan, blocking=False)
                raise PlanRequiresBackfill()
            elif not self.prod_plan.missing_intervals:
                return self._deploy_to_prod_and_merge()
            else:
                raise RuntimeError("Got to an unexpected state trying to deploy prod.")
        if self.action.is_get_pull_request_number:
            # the output is captured by the action and stored in a variable
            print(self.pull_request_info.pr_number)
        if self.action.is_delete_environment:
            # TODO: get this dynamically
            schemas = ["db", "cleansed"]
            for schema in schemas:
                self.context.engine_adapter.drop_schema(
                    schema_name="__".join([schema, self.pr_environment_name]),
                    ignore_if_not_exists=True,
                    cascade=True,
                )
            return
        if self.action.is_get_merge_commit_sha:
            # the output is captured by the action and stored in a variable
            print(self.pull_request.merge_commit_sha)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Github Action Bot")
    parser.add_argument(
        "--github_token",
        help="The access token to use when connecting to Github",
    )
    parser.add_argument(
        "--github_base_url",
        help="The base URL to use for Github. Only needs to be set for Github Enterprise users.",
    )
    parser.add_argument(
        "--sqlmesh_root_dir",
        default=os.getcwd(),
        help="Path to the models directory.",
    )
    parser.add_argument(
        "--config_name",
        help="Name of the config object.",
    )
    parser.add_argument(
        "--event_payload_path",
        help="The path to the Github event payload",
    )
    parser.add_argument("--action", help="The action to take")
    args = parser.parse_args()
    bot = GithubPullRequestBot.create(
        sqlmesh_root_dir=args.sqlmesh_root_dir,
        config_name=args.config_name,
        github_token=args.github_token,
        github_base_url=args.github_base_url,
        event_payload_path=pathlib.Path(args.event_payload_path),
        action=Action[args.action.upper()],
    )
    bot.process()
