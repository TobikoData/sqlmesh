from sqlmesh.integrations.github.notification_target import GithubNotificationTarget


def test_pull_request_info():
    target = GithubNotificationTarget(
        token="XXXX", pull_request_url="https://api.github.com/repos/owner/repo/pulls/2"
    )
    assert target.pull_request_info == {
        "owner": "owner",
        "repo": "repo",
        "pr_number": "2",
        "full_repo_path": "owner/repo",
    }
