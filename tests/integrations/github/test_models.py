from sqlmesh.integrations.github.models import PullRequestInfo


def test_pull_request_info():
    pr_info = PullRequestInfo(
        owner="owner",
        repo="repo",
        pr_number="2",
    )
    assert pr_info == PullRequestInfo.create_from_pull_request_url(
        "https://api.github.com/repos/owner/repo/pulls/2"
    )
    assert pr_info.full_repo_path == "owner/repo"
