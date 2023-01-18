from __future__ import annotations

import datetime
import typing as t

from sqlmesh.core.notification_target import NotificationStatus
from sqlmesh.core.user import User
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from github.Repository import Repository

NOTIFICATION_STATUS_TO_EMOJI = {
    NotificationStatus.FAILURE: ":x:",
    NotificationStatus.WARNING: ":bangbang:",
    NotificationStatus.INFO: ":information_source:",
    NotificationStatus.PROGRESS: ":rocket:",
    NotificationStatus.SUCCESS: ":white_check_mark:",
}


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
        _, _, _, _, owner, repo, _, pr_number = pull_request_url.split("/")
        return cls(
            owner=owner,
            repo=repo,
            pr_number=pr_number,
        )


def add_comment_to_pr(
    repo: Repository,
    pull_request_info: PullRequestInfo,
    notification_status: NotificationStatus,
    msg: str,
    user_to_append_to: t.Optional[User] = None,
) -> None:
    emoji = NOTIFICATION_STATUS_TO_EMOJI[notification_status]
    msg = f"{datetime.datetime.utcnow().isoformat(sep=' ', timespec='seconds')} - {emoji} {msg} {emoji}"
    issue = repo.get_issue(pull_request_info.pr_number)
    if user_to_append_to:
        for comment in issue.get_comments():
            if comment.user.name == user_to_append_to.github_username:
                comment.edit("\n".join([comment.body, msg]))
                return
    issue.create_comment(msg)
