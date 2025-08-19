import pytest

pytestmark = pytest.mark.gitlab


def test_merge_request_event(make_event_from_fixture):
    # Placeholder for GitLab merge request event
    # You'll need to create a corresponding GitLab merge request JSON fixture
    event = make_event_from_fixture("tests/fixtures/gitlab/merge_request.json")
    assert event.is_merge_request
    # Add more GitLab-specific assertions here


def test_merge_request_comment_event(make_event_from_fixture):
    # Placeholder for GitLab merge request comment event
    # You'll need to create a corresponding GitLab merge request comment JSON fixture
    event = make_event_from_fixture("tests/fixtures/gitlab/merge_request_comment.json")
    assert event.is_comment
    # Add more GitLab-specific assertions here


def test_merge_request_info(make_event_from_fixture, make_pull_request_info):
    # Placeholder for GitLab merge request info
    # You'll need to create a corresponding GitLab merge request JSON fixture
    merge_request_info = make_pull_request_info(
        make_event_from_fixture("tests/fixtures/gitlab/merge_request.json")
    )
    # Add GitLab-specific assertions for owner, repo, merge_request_number, etc.
    # assert merge_request_info.owner == "..."
    # assert merge_request_info.repo == "..."
    # assert merge_request_info.pr_number == ... # or equivalent for GitLab
