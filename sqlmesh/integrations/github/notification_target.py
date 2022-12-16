from __future__ import annotations

import sys
import typing as t

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from sqlmesh.core.notification_target import BaseNotificationTarget
from sqlmesh.integrations.github.shared import PullRequestInfo, add_comment_to_pr


class GithubNotificationTarget(BaseNotificationTarget):
    """
    Github Notification Target that sends notifications to pull requests
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
        add_comment_to_pr(
            repo, self.pull_request_info, msg, username_to_append_to="SQLMesh"
        )

    @property
    def pull_request_info(self) -> PullRequestInfo:
        if not self._pull_request_info:
            self._pull_request_info = PullRequestInfo.create_from_pull_request_url(
                self.pull_request_url
            )
        return self._pull_request_info
