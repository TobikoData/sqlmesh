"""
WIP - This is just early testing code to get a working CI/CD flow working
"""
import argparse
import json
import os
import pathlib
import typing as t

from sqlmesh import Context
from sqlmesh.core import constants as c
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget
from sqlmesh.utils.pydantic import PydanticModel


class MissingARequiredApprover(Exception):
    pass


class SQLMeshUser(PydanticModel):
    """
    Temporary placeholder to a "SQLMeshUser" where we would register things like username info
    """

    handle: str
    github_login: t.Optional[str] = None
    slack_handle: t.Optional[str] = None
    email: t.Optional[str] = None


USERS = [
    SQLMeshUser(handle="demo_bot", github_login="SQLMesh"),
]

GATEKEEPER_GITHUB_LOGINS: t.Set[str] = {
    user.github_login.lower() for user in USERS if user.github_login is not None
}


PullRequestPayload = t.Dict[str, t.Any]


def _get_payload() -> PullRequestPayload:
    path = pathlib.Path(os.environ["GITHUB_EVENT_PATH"])
    return json.loads(path.read_text())


def is_review_payload(payload: PullRequestPayload) -> bool:
    return bool(payload.get("review"))


def has_gatekeeper_approval(payload: PullRequestPayload) -> bool:
    if not is_review_payload(payload):
        return False
    if payload["review"]["state"].lower() != "approved":
        return False
    approver = payload["review"]["user"]["login"].lower()
    if approver in GATEKEEPER_GITHUB_LOGINS:
        return True
    return False


def get_pull_request_url(payload: PullRequestPayload) -> str:
    return payload["review"]["pull_request_url"]


def trigger_backfill(payload: PullRequestPayload) -> None:
    config = os.environ["SQLMESH_CONFIG_NAME"]
    github_token = os.environ["GITHUB_PAT"]
    notification_target = GithubNotificationTarget(
        token=github_token, pull_request_url=get_pull_request_url(payload)
    )
    context = Context(config=config, notification_targets=[notification_target])
    plan = context.plan(environment=c.PROD)
    # Check if the plan is backfill plan
    context.apply(plan)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Github Action Bot")
    parser.add_argument(
        "--check_gatekeeper_approval",
        action="store_true",
        help="Check if the PR as required approvals",
    )
    args = parser.parse_args()
    payload = _get_payload()
    if args.check_gatekeeper_approval:
        if not has_gatekeeper_approval(payload):
            raise MissingARequiredApprover(
                f"Required Approvers: {', '.join(GATEKEEPER_GITHUB_LOGINS)}"
            )
        trigger_backfill(payload)
