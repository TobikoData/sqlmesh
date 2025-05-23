import typing as t

import pytest
from pytest_mock.plugin import MockerFixture

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
def make_pull_request_review() -> t.Callable:
    from github.PullRequestReview import PullRequestReview

    def _make_function(username: str, state: str, **kwargs) -> PullRequestReview:
        return PullRequestReview(
            "test",  # type: ignore
            {},
            {
                # Name is whatever they provide in their GitHub profile or login as fallback. Always use login.
                "user": AttributeDict(name="Unrelated", login=username),
                "state": state,
                **kwargs,
            },
            completed=False,
        )

    return _make_function


@pytest.fixture
def make_controller(mocker: MockerFixture, copy_to_temp_path: t.Callable) -> t.Callable:
    from github import Github

    def _make_function(
        event_path: t.Union[str, t.Dict],
        client: Github,
        *,
        merge_state_status: MergeStateStatus = MergeStateStatus.CLEAN,
        bot_config: t.Optional[GithubCICDBotConfig] = None,
        mock_out_context: bool = True,
        config: t.Optional[t.Union[Config, str]] = None,
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

        paths = copy_to_temp_path("examples/sushi")

        orig_console = get_console()
        try:
            set_console(MarkdownConsole())

            return GithubController(
                paths=paths,
                token="abc",
                event=(
                    GithubEvent.from_path(event_path)
                    if isinstance(event_path, str)
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
