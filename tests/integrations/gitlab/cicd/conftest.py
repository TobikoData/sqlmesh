import typing as t
import json

import pytest
from pytest_mock.plugin import MockerFixture
from pathlib import Path

from sqlmesh.core.config import Config
from sqlmesh.core.console import set_console, get_console, MarkdownConsole
from sqlmesh.integrations.gitlab.cicd.config import GitlabCICDBotConfig
from sqlmesh.integrations.gitlab.cicd.controller import (
    GitlabController,
    GitlabEvent,
)
from sqlglot.helper import ensure_list


@pytest.fixture
def gitlab_client(mocker: MockerFixture):
    from gitlab import Gitlab
    from gitlab.v4.objects import Project, ProjectMergeRequest

    client_mock = mocker.MagicMock(spec=Gitlab)
    mocker.patch("gitlab.Gitlab", client_mock)

    mock_project = mocker.MagicMock(spec=Project)
    client_mock.projects.get.return_value = mock_project

    mock_merge_request = mocker.MagicMock(spec=ProjectMergeRequest)
    mock_merge_request.target_branch = "main"
    mock_project.mergerequests.get.return_value = mock_merge_request

    return client_mock


@pytest.fixture
def sqlmesh_repo_root_path() -> Path:
    return next(p for p in Path(__file__).parents if str(p).endswith("tests")).parent


@pytest.fixture
def make_controller(
    mocker: MockerFixture,
    copy_to_temp_path: t.Callable,
    monkeypatch: pytest.MonkeyPatch,
    sqlmesh_repo_root_path: Path,
) -> t.Callable:
    from gitlab import Gitlab

    def _make_function(
        event_path: t.Union[str, Path, t.Dict],
        client: Gitlab,
        *,
        bot_config: t.Optional[GitlabCICDBotConfig] = None,
        mock_out_context: bool = True,
        config: t.Optional[t.Union[Config, str]] = None,
        paths: t.Optional[t.Union[Path, t.List[Path]]] = None,
    ) -> GitlabController:
        if mock_out_context:
            mocker.patch("sqlmesh.core.context.Context.apply", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())
        if bot_config:
            mocker.patch(
                "sqlmesh.integrations.gitlab.cicd.controller.GitlabController.bot_config",
                bot_config,
            )

        if paths is None:
            paths = copy_to_temp_path(sqlmesh_repo_root_path / "examples" / "sushi")

        paths = ensure_list(paths)

        if isinstance(event_path, str):
            as_path = Path(event_path)
            if not as_path.is_absolute():
                event_path = sqlmesh_repo_root_path / as_path

        monkeypatch.chdir(paths[0])

        monkeypatch.setenv("GITLAB_CI", "true")
        monkeypatch.setenv("CI_PROJECT_ID", "123")
        monkeypatch.setenv("CI_MERGE_REQUEST_IID", "1")

        orig_console = get_console()
        try:
            set_console(MarkdownConsole(warning_capture_only=True, error_capture_only=True))

            return GitlabController(
                paths=paths,
                token="abc",
                event=(
                    GitlabEvent(json.load(open(event_path, "r", encoding="utf-8")))
                    if isinstance(event_path, (str, Path))
                    else GitlabEvent(event_path)
                ),
                client=client,
                config=config,
            )

        finally:
            set_console(orig_console)

    return _make_function


@pytest.fixture
def make_event_note() -> t.Callable:
    def _make_function(note: str) -> t.Dict:
        return {
            "object_kind": "note",
            "event_type": "note",
            "object_attributes": {"note": note},
        }

    return _make_function


class MockMergeRequestComment:
    def __init__(self, body: str):
        self.body = body


@pytest.fixture
def make_mock_merge_request_comment() -> t.Callable:
    def _make_function(
        comment: str, created_comments: t.Optional[t.List[MockMergeRequestComment]] = None
    ) -> MockMergeRequestComment:
        mock_comment = MockMergeRequestComment(body=comment)
        if created_comments is not None:
            created_comments.append(mock_comment)
        return mock_comment

    return _make_function


class MockCommitStatus:
    def __init__(self, state: str, description: str, target_url: str, name: str):
        self.state = state
        self.description = description
        self.target_url = target_url
        self.name = name


@pytest.fixture
def make_mock_check_run() -> t.Callable:
    def _make_function(
        state: str, description: str, target_url: str, name: str, **kwargs
    ) -> MockCommitStatus:
        return MockCommitStatus(
            state=state, description=description, target_url=target_url, name=name
        )

    return _make_function


class MockMergeRequestApproval:
    def __init__(self, username: str, state: str):
        self.username = username
        self.state = state


@pytest.fixture
def make_merge_request_approval() -> t.Callable:
    def _make_function(username: str, state: str) -> MockMergeRequestApproval:
        return MockMergeRequestApproval(username=username, state=state)

    return _make_function
