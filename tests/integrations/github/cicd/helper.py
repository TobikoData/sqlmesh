import os
import typing as t
from unittest import mock

from pytest_mock.plugin import MockerFixture

from sqlmesh.integrations.github.cicd.controller import GithubController, GithubEvent


def get_mocked_controller(
    event: GithubEvent, mocker: MockerFixture, config: t.Optional[str] = None
) -> GithubController:
    mocker.patch("github.Github", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
    mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())
    with mock.patch.dict(
        os.environ, {"GITHUB_API_URL": "https://api.github.com/repos/Codertocat/Hello-World"}
    ):
        controller = GithubController(
            paths=["examples/sushi"], token="abc", event=event, config=config
        )
        controller._console = mocker.MagicMock()
        return controller
