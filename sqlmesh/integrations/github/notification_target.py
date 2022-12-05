import sys
import typing as t

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from sqlmesh.core.notification_target import BaseNotificationTarget
from sqlmesh.integrations.github.models import PullRequestInfo


class GithubNotificationTarget(BaseNotificationTarget):
    """
    Github Notification Target. Does not currently support built-in scheduler.
    """

    kind: Literal["github"] = "github"
    token: str
    github_url: t.Optional[str] = None
    pull_request_url: str
    _pull_request_info: t.Optional[PullRequestInfo] = None

    def send(self, msg: str, **kwargs) -> None:
        from github import Github

        client = (
            Github(login_or_token=self.token, base_url=self.github_url)
            if self.github_url
            else Github(login_or_token=self.token)
        )
        repo = client.get_repo(self.pull_request_info.full_repo_path, lazy=True)
        issue = repo.get_issue(self.pull_request_info.pr_number)
        for comment in issue.get_comments():
            # TODO: Make this more robust
            if comment.user.name == "SQLMesh":
                comment.edit("\n".join([comment.body, msg]))
                break
        else:
            issue.create_comment(msg)

    @property
    def pull_request_info(self) -> PullRequestInfo:
        if not self._pull_request_info:
            self._pull_request_info = PullRequestInfo.create_from_pull_request_url(
                self.pull_request_url
            )
        return self._pull_request_info
