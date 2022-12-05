import sys
import typing as t

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from sqlmesh.core.notification_target import BaseNotificationTarget


class GithubNotificationTarget(BaseNotificationTarget):
    """
    Github Notification Target. Does not currently support built-in scheduler.
    """

    kind: Literal["github"] = "github"
    token: str
    pull_request_url: str

    def send(self, msg: str, **kwargs) -> None:
        raise NotImplementedError

    @property
    def pull_request_info(self) -> t.Dict[str, str]:
        _, _, _, _, owner, repo, _, pr_number = self.pull_request_url.split("/")
        return {
            "owner": owner,
            "repo": repo,
            "pr_number": pr_number,
            "full_repo_path": "/".join([owner, repo]),
        }
