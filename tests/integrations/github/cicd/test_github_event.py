import pytest

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]
pytestmark = pytest.mark.github


def test_pull_request_review_submit_event(make_event_from_fixture):
    event = make_event_from_fixture("tests/fixtures/github/pull_request_review_submit.json")
    assert not event.is_pull_request
    assert event.is_review
    assert event.pull_request_url == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"


def test_pull_request_synchronized_event(make_event_from_fixture):
    event = make_event_from_fixture("tests/fixtures/github/pull_request_synchronized.json")
    assert event.is_pull_request
    assert event.pull_request_url == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"


def test_github_pull_request_comment(make_event_from_fixture):
    event = make_event_from_fixture("tests/fixtures/github/pull_request_comment.json")
    assert event.is_comment
    assert event.pull_request_url == "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"
    assert event.pull_request_comment_body == "example_comment"


def test_pull_request_synchronized_info(make_event_from_fixture, make_pull_request_info):
    pull_request_info = make_pull_request_info(
        make_event_from_fixture("tests/fixtures/github/pull_request_synchronized.json")
    )
    assert pull_request_info.owner == "Codertocat"
    assert pull_request_info.repo == "Hello-World"
    assert pull_request_info.pr_number == 2
    assert pull_request_info.full_repo_path == "Codertocat/Hello-World"


def test_pull_request_synchronized_enterprise(make_event_from_fixture, make_pull_request_info):
    pull_request_info = make_pull_request_info(
        make_event_from_fixture("tests/fixtures/github/pull_request_synchronized_enterprise.json")
    )
    assert pull_request_info.owner == "org"
    assert pull_request_info.repo == "repo"
    assert pull_request_info.pr_number == 3
    assert pull_request_info.full_repo_path == "org/repo"
