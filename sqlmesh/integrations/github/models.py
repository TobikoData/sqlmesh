from __future__ import annotations

from sqlmesh.utils.pydantic import PydanticModel


class PullRequestInfo(PydanticModel):
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
