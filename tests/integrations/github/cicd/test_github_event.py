from sqlmesh.integrations.github.cicd.controller import GithubEvent, PullRequestInfo

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]


def test_pull_request_review_submit_event(github_pull_request_review_submit_event: GithubEvent):
    assert not github_pull_request_review_submit_event.is_pull_request
    assert github_pull_request_review_submit_event.is_review
    assert (
        github_pull_request_review_submit_event.pull_request_url
        == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"
    )


def test_pull_request_synchronized_event(github_pull_request_synchronized_event: GithubEvent):
    assert github_pull_request_synchronized_event.is_pull_request
    assert (
        github_pull_request_synchronized_event.pull_request_url
        == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"
    )


def test_github_pull_request_comment(github_pull_request_comment_event: GithubEvent):
    assert github_pull_request_comment_event.is_comment
    assert (
        github_pull_request_comment_event.pull_request_url
        == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"
    )
    assert github_pull_request_comment_event.pull_request_comment_body == "example_comment"


def test_pull_request_synchronized_info(github_pull_request_synchronized_info: PullRequestInfo):
    assert github_pull_request_synchronized_info.owner == "Codertocat"
    assert github_pull_request_synchronized_info.repo == "Hello-World"
    assert github_pull_request_synchronized_info.pr_number == 2
    assert github_pull_request_synchronized_info.full_repo_path == "Codertocat/Hello-World"
