import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from pathlib import Path

from sqlmesh.core.config import Config
from sqlmesh.core.console import set_console, get_console, MarkdownConsole
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.integrations.github.cicd.controller import (
    GithubController,
    GithubEvent,
    MergeStateStatus,
    PullRequestInfo,
)
from sqlmesh.utils import AttributeDict
from sqlglot.helper import ensure_list


@pytest.fixture
def github_client(mocker: MockerFixture):
    from github import Github
    from github.Issue import Issue
    from github.PullRequest import PullRequest
    from github.PullRequestReview import PullRequestReview
    from github.Repository import Repository

    client_mock = mocker.MagicMock(spec=Github)
    mocker.patch("github.Github", client_mock)

    mock_repository = mocker.MagicMock(spec=Repository)
    client_mock.get_repo.return_value = mock_repository

    mock_pull_request = mocker.MagicMock(spec=PullRequest)
    mock_pull_request.base.ref = "main"
    mock_pull_request.get_reviews.return_value = [mocker.MagicMock(spec=PullRequestReview)]
    mock_repository.get_pull.return_value = mock_pull_request
    mock_repository.get_issue.return_value = mocker.MagicMock(spec=Issue)

    return client_mock


@pytest.fixture
def make_pull_request_review(github_client) -> t.Callable:
    from github.PullRequestReview import PullRequestReview

    def _make_function(username: str, state: str, **kwargs) -> PullRequestReview:
        return PullRequestReview(
            github_client.requester,
            {},
            {
                # Name is whatever they provide in their GitHub profile or login as a fallback. Always use login.
                "user": AttributeDict(name="Unrelated", login=username),
                "state": state,
                **kwargs,
            },
        )

    return _make_function


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
    from github import Github

    def _make_function(
        event_path: t.Union[str, Path, t.Dict],
        client: Github,
        *,
        merge_state_status: MergeStateStatus = MergeStateStatus.CLEAN,
        bot_config: t.Optional[GithubCICDBotConfig] = None,
        mock_out_context: bool = True,
        config: t.Optional[t.Union[Config, str]] = None,
        paths: t.Optional[t.Union[Path, t.List[Path]]] = None,
    ) -> GithubController:
        if mock_out_context:
            mocker.patch("sqlmesh.core.context.Context.apply", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_plan_tests", mocker.MagicMock())
            mocker.patch("sqlmesh.core.context.Context._run_tests", mocker.MagicMock())
        mocker.patch(
            "sqlmesh.integrations.github.cicd.controller.GithubController._get_merge_state_status",
            mocker.MagicMock(side_effect=lambda: merge_state_status),
        )
        if bot_config:
            mocker.patch(
                "sqlmesh.integrations.github.cicd.controller.GithubController.bot_config",
                bot_config,
            )

        if paths is None:
            paths = copy_to_temp_path(sqlmesh_repo_root_path / "examples" / "sushi")

        paths = ensure_list(paths)

        if isinstance(event_path, str):
            # resolve relative event_path references to absolute so they dont get affected by chdir() below
            as_path = Path(event_path)
            if not as_path.is_absolute():
                event_path = sqlmesh_repo_root_path / as_path

        # set the current working directory to the temp path so that config references to eg duckdb "db.db"
        # get created in the temp path and not in the SQLMesh repo root path that the tests are triggered from
        monkeypatch.chdir(paths[0])

        # make the tests think they are running in GitHub Actions
        monkeypatch.setenv("GITHUB_ACTIONS", "true")

        orig_console = get_console()
        try:
            set_console(MarkdownConsole(warning_capture_only=True, error_capture_only=True))

            return GithubController(
                paths=paths,
                token="abc",
                event=(
                    GithubEvent.from_path(event_path)
                    if isinstance(event_path, (str, Path))
                    else GithubEvent.from_obj(event_path)
                ),
                client=client,
                config=config,
            )

        finally:
            set_console(orig_console)

    return _make_function


@pytest.fixture
def make_event_issue_comment() -> t.Callable:
    def _make_function(action: str, comment: str) -> t.Dict:
        return {
            "action": action,
            "comment": {"body": comment},
            "issue": {
                "pull_request": {
                    "url": "https://api.github.com/repos/Codertocat/Hello-World/pulls/2"
                }
            },
        }

    return _make_function


class MockIssueComment:
    def __init__(self, body: str):
        self.body = body

    def edit(self, body):
        self.body = body


@pytest.fixture
def make_mock_issue_comment() -> t.Callable:
    def _make_function(
        comment: str, created_comments: t.Optional[t.List[MockIssueComment]] = None
    ) -> MockIssueComment:
        mock_issue_comment = MockIssueComment(body=comment)
        if created_comments is not None:
            created_comments.append(mock_issue_comment)
        return mock_issue_comment

    return _make_function


class MockCheckRun:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.previous_kwargs = []

    def edit(self, **kwargs):
        self.previous_kwargs.append(self.kwargs.copy())
        self.kwargs = {**self.kwargs, **kwargs}

    @property
    def all_kwargs(self) -> t.List[t.Dict]:
        return self.previous_kwargs + [self.kwargs]


@pytest.fixture
def make_mock_check_run() -> t.Callable:
    def _make_function(**kwargs) -> MockCheckRun:
        return MockCheckRun(**kwargs)

    return _make_function


@pytest.fixture
def make_event_from_fixture() -> t.Callable:
    def _make_function(fixture_path: str) -> GithubEvent:
        return GithubEvent.from_path(fixture_path)

    return _make_function


@pytest.fixture
def make_pull_request_info() -> t.Callable:
    def _make_function(event: GithubEvent) -> PullRequestInfo:
        return PullRequestInfo.create_from_pull_request_url(event.pull_request_url)

    return _make_function
