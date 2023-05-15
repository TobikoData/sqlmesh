import os
from unittest import mock

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.integrations.github.cicd.controller import (
    GithubController,
    GithubEvent,
    PullRequestInfo,
)


@pytest.fixture
def github_pull_request_review_submit_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_review_submit.json")


@pytest.fixture
def github_pull_request_synchronized_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_synchronized.json")


@pytest.fixture
def github_pull_request_comment_event() -> GithubEvent:
    return GithubEvent.from_path("tests/fixtures/github/pull_request_comment.json")


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
    mocker.patch("github.Github", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())
    with mock.patch.dict(
        os.environ, {"GITHUB_API_URL": "https://api.github.com/repos/Codertocat/Hello-World"}
    ):
        controller = GithubController(
            paths=["examples/sushi"],
            config="required_approvers_config",
            token="abc",
            event=github_pull_request_synchronized_event,
        )
        controller._console = mocker.MagicMock()
        return controller


@pytest.fixture
def github_pr_closed_controller(
    github_pull_request_closed_event: GithubEvent, mocker: MockerFixture
):
    mocker.patch("github.Github", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())
    with mock.patch.dict(
        os.environ, {"GITHUB_API_URL": "https://api.github.com/repos/Codertocat/Hello-World"}
    ):
        controller = GithubController(
            paths=["examples/sushi"],
            token="abc",
            event=github_pull_request_closed_event,
        )
        controller._console = mocker.MagicMock()
        return controller
