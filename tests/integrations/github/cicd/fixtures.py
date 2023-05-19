import json
import typing as t

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.integrations.github.cicd.controller import (
    GithubController,
    GithubEvent,
    PullRequestInfo,
)
from tests.integrations.github.cicd.helper import get_mocked_controller


@pytest.fixture
def github_pull_request_review_submit_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_review_submit.json")


@pytest.fixture
def github_pull_request_synchronized_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_synchronized.json")


@pytest.fixture
def github_pull_request_comment_raw() -> t.Dict[str, t.Any]:
    with open("tests/fixtures/github/pull_request_comment.json") as f:
        return json.load(f)


@pytest.fixture
def github_pull_request_comment_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_comment.json")


@pytest.fixture
def github_pull_request_command_deploy_prod_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_command_deploy.json")


@pytest.fixture
def github_pull_request_closed_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_closed.json")


@pytest.fixture
def github_pull_request_synchronized_info(
    github_pull_request_synchronized_event: GithubEvent,
) -> PullRequestInfo:
    return PullRequestInfo.create_from_pull_request_url(
        github_pull_request_synchronized_event.pull_request_url
    )


@pytest.fixture
def github_pr_synchronized_approvers_controller(
    github_pull_request_synchronized_event: GithubEvent, mocker: MockerFixture
) -> GithubController:
    return get_mocked_controller(
        github_pull_request_synchronized_event, mocker, config="required_approvers_config"
    )


@pytest.fixture
def github_pr_closed_controller(
    github_pull_request_closed_event: GithubEvent, mocker: MockerFixture
):
    return get_mocked_controller(github_pull_request_closed_event, mocker)


@pytest.fixture
def github_pr_invalid_command_controller(
    github_pull_request_comment_event: GithubEvent, mocker: MockerFixture
):
    return get_mocked_controller(github_pull_request_comment_event, mocker)


@pytest.fixture
def github_pr_command_deploy_prod_controller(
    github_pull_request_command_deploy_prod_event: GithubEvent, mocker: MockerFixture
):
    return get_mocked_controller(github_pull_request_command_deploy_prod_event, mocker)
