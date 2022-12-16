from __future__ import annotations

import typing as t

from github.Repository import Repository

from sqlmesh.utils.pydantic import PydanticModel


class PullRequestInfo(PydanticModel):
    """Contains information related to a pull request that can be used to construct other objects/URLs"""

    owner: str
    repo: str
    pr_number: int

    @property
    def full_repo_path(self):
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
    msg: str,
    username_to_append_to: t.Optional[str] = None,
) -> None:
    issue = repo.get_issue(pull_request_info.pr_number)
    if username_to_append_to:
        for comment in issue.get_comments():
            if comment.user.name == username_to_append_to:
                comment.edit("\n".join([comment.body, msg]))
                return
    issue.create_comment(msg)
