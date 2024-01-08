# type: ignore
import os
import pathlib
from unittest import mock
from unittest.mock import PropertyMock, call

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import constants as c
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.model import SqlModel
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.integrations.github.cicd.controller import (
    BotCommand,
    GithubCheckStatus,
    MergeStateStatus,
)
from tests.integrations.github.cicd.fixtures import MockIssueComment

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]
pytestmark = pytest.mark.github

github_controller_approvers_params = [
    (
        "2 approvers, 1 required",
        [
            {
                "username": "required_approver",
                "state": "APPROVED",
            },
            {
                "username": "non_required_approver",
                "state": "APPROVED",
            },
        ],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[UserRole.REQUIRED_APPROVER],
            ),
        ],
        True,
        True,
    ),
    (
        "2 reviewers, 1 approved and required, 1 disapproved and not required",
        [
            {
                "username": "required_approver",
                "state": "APPROVED",
            },
            {
                "username": "non_required_approver",
                "state": "CHANGES_REQUESTED",
            },
        ],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[UserRole.REQUIRED_APPROVER],
            ),
        ],
        True,
        True,
    ),
    (
        "2 reviewers, 1 disapproved and required, 1 approved and not required",
        [
            {
                "username": "required_approver",
                "state": "CHANGES_REQUESTED",
            },
            {
                "username": "non_required_approver",
                "state": "APPROVED",
            },
        ],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[UserRole.REQUIRED_APPROVER],
            ),
        ],
        False,
        True,
    ),
    (
        "1 reviewer, 1 approved and not required",
        [
            {
                "username": "non_required_approver",
                "state": "APPROVED",
            },
        ],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[UserRole.REQUIRED_APPROVER],
            ),
        ],
        False,
        True,
    ),
    (
        "1 reviewer, 1 disapproved and no required approvers",
        [
            {
                "username": "non_required_approver",
                "state": "CHANGES_REQUESTED",
            },
        ],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[],
            ),
        ],
        True,
        False,
    ),
    (
        "No reviews and 1 required approver",
        [],
        [
            User(
                username="test",
                github_username="required_approver",
                roles=[UserRole.REQUIRED_APPROVER],
            ),
        ],
        False,
        True,
    ),
    (
        "No reviews and no required approvers",
        [],
        [],
        True,
        False,
    ),
]


@pytest.mark.parametrize(
    "reviews, users, has_required_approval, do_required_approval_check",
    [test[1:] for test in github_controller_approvers_params],
    ids=[test[0] for test in github_controller_approvers_params],
)
def test_github_controller_approvers(
    reviews,
    users,
    has_required_approval,
    do_required_approval_check,
    github_client,
    make_pull_request_review,
    make_controller,
    mocker: MockerFixture,
):
    mock_pull_request = github_client.get_repo().get_pull()
    mock_pull_request.get_reviews = mocker.MagicMock(
        side_effect=lambda: [make_pull_request_review(**review) for review in reviews]
    )

    controller = make_controller(
        "tests/fixtures/github/pull_request_review_submit.json", github_client
    )
    controller._context.users = users
    assert controller.has_required_approval == has_required_approval
    assert controller.do_required_approval_check == do_required_approval_check


is_comment_triggered_params = [
    (
        "comment is created",
        "created",
        "testing",
        True,
    ),
    (
        "comment is edited",
        "edited",
        "testing",
        True,
    ),
    (
        "comment is deleted",
        "deleted",
        "testing",
        False,
    ),
]


@pytest.mark.parametrize(
    "action, comment, is_comment_triggered",
    [test[1:] for test in is_comment_triggered_params],
    ids=[test[0] for test in is_comment_triggered_params],
)
def test_is_comment(
    action, comment, is_comment_triggered, github_client, make_event_issue_comment, make_controller
):
    controller = make_controller(make_event_issue_comment(action, comment), github_client)
    assert controller.is_comment_added == is_comment_triggered


def test_pr_environment_name(github_client, make_controller):
    controller = make_controller(
        "tests/fixtures/github/pull_request_review_submit.json", github_client
    )
    assert controller.pr_environment_name == "hello_world_2"


def test_pr_plan(github_client, make_controller):
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    assert controller.pr_plan.environment.name == "hello_world_2"
    assert controller.pr_plan.skip_backfill
    assert not controller.pr_plan.no_gaps
    assert not controller._context.apply.called
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)
    assert (
        controller._pr_plan_builder._categorizer_config
        == controller._context.auto_categorize_changes
    )


def test_pr_plan_auto_categorization(github_client, make_controller):
    custom_categorizer_config = CategorizerConfig.all_semi()
    default_start = "1 week ago"
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            auto_categorize_changes=custom_categorizer_config, default_pr_start=default_start
        ),
    )
    assert controller.pr_plan.environment.name == "hello_world_2"
    assert controller.pr_plan.skip_backfill
    assert not controller.pr_plan.no_gaps
    assert not controller._context.apply.called
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)
    assert controller._pr_plan_builder._categorizer_config == custom_categorizer_config
    assert controller.pr_plan.start == default_start


def test_prod_plan(github_client, make_controller):
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )

    assert controller.prod_plan.environment.name == c.PROD
    assert not controller.prod_plan.skip_backfill
    assert controller.prod_plan.no_gaps
    assert not controller._context.apply.called
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)
    assert (
        controller._prod_plan_builder._categorizer_config
        == controller._context.auto_categorize_changes
    )


def test_prod_plan_auto_categorization(github_client, make_controller):
    custom_categorizer_config = CategorizerConfig.all_off()
    default_pr_start = "1 week ago"
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            auto_categorize_changes=custom_categorizer_config, default_pr_start=default_pr_start
        ),
    )

    assert controller.prod_plan.environment.name == c.PROD
    assert not controller.prod_plan.skip_backfill
    assert controller.prod_plan.no_gaps
    assert not controller._context.apply.called
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)
    assert controller._prod_plan_builder._categorizer_config == custom_categorizer_config
    # default PR start should be ignored for prod plans
    assert controller.prod_plan.start != default_pr_start


def test_prod_plan_with_gaps(github_client, make_controller):
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )

    assert controller.prod_plan_with_gaps.environment.name == c.PROD
    assert not controller.prod_plan_with_gaps.skip_backfill
    assert not controller._prod_plan_with_gaps_builder._auto_categorization_enabled
    assert not controller.prod_plan_with_gaps.no_gaps
    assert not controller._context.apply.called
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)


def test_run_tests(github_client, make_controller):
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    controller.run_tests()
    assert controller._context._run_tests.called


update_sqlmesh_comment_info_params = [
    (
        "No comments, one is created and then updated with value",
        [],
        "test1",
        None,
        "**SQLMesh Bot Info**\ntest1",
        "**SQLMesh Bot Info**\ntest1",
    ),
    (
        "Existing comments that are not related, one is created and then updated with value",
        [
            MockIssueComment(body="test2"),
            MockIssueComment(body="test3"),
        ],
        "test1",
        None,
        "**SQLMesh Bot Info**\ntest1",
        "**SQLMesh Bot Info**\ntest1",
    ),
    (
        "Existing bot comment, that is updated and create comment is not called",
        [
            MockIssueComment(body="**SQLMesh Bot Info**\ntest2"),
            MockIssueComment(body="test3"),
        ],
        "test1",
        None,
        "**SQLMesh Bot Info**\ntest2\ntest1",
        None,
    ),
    (
        "Existing bot comment, that is not updated because of dedup_regex and create comment is not called",
        [
            MockIssueComment(body="**SQLMesh Bot Info**\ntest2"),
            MockIssueComment(body="test3"),
        ],
        "test1",
        "test2",
        "**SQLMesh Bot Info**\ntest2",
        None,
    ),
    (
        "Ensure comments are truncated if they are too long",
        [
            MockIssueComment(body="**SQLMesh Bot Info**\ntest1"),
        ],
        # Making sure that although we will be under the character limit of `65535` we will still truncate
        # because the byte size of this character is 3 and therefore we will be over the limit since it is based
        # on bytes on not characters (despite what the error message may say)
        "桜" * 65000,
        None,
        # ((Max Byte Length) - (Length of "**SQLMesh Bot Info**\ntest1\n")) / (Length of "桜")
        "**SQLMesh Bot Info**\ntest1\n" + ("桜" * int((65535 - 27) / 3)),
        None,
    ),
]


@pytest.mark.parametrize(
    "existing_comments, comment, dedup_regex, resulting_comment, create_comment",
    [test[1:] for test in update_sqlmesh_comment_info_params],
    ids=[test[0] for test in update_sqlmesh_comment_info_params],
)
def test_update_sqlmesh_comment_info(
    existing_comments,
    comment,
    dedup_regex,
    resulting_comment,
    create_comment,
    github_client,
    make_mock_issue_comment,
    make_controller,
    mocker: MockerFixture,
):
    mock_issue = github_client.get_repo().get_issue()
    created_comments = []
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: existing_comments)
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda body: make_mock_issue_comment(body, created_comments)
    )

    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    updated, resp = controller.update_sqlmesh_comment_info(comment, dedup_regex=dedup_regex)
    assert resp.body == resulting_comment
    if create_comment is None:
        assert len(created_comments) == 0
    else:
        assert len(created_comments) == 1
        assert created_comments[0].body == create_comment


def test_deploy_to_prod_merge_error(github_client, make_controller):
    mock_pull_request = github_client.get_repo().get_pull()
    mock_pull_request.merged = True
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    with pytest.raises(
        Exception,
        match=r"^PR is already merged and this event was triggered prior to the merge.$",
    ):
        controller.deploy_to_prod()


def test_deploy_to_prod_dirty_pr(github_client, make_controller):
    mock_pull_request = github_client.get_repo().get_pull()
    mock_pull_request.merged = False
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        merge_state_status=MergeStateStatus.DIRTY,
    )
    with pytest.raises(Exception, match=r"^Merge commit cannot be cleanly created.*"):
        controller.deploy_to_prod()


def test_try_invalidate_pr_environment(github_client, make_controller, mocker: MockerFixture):
    invalidate_controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    invalidate_controller._context._state_sync = mocker.MagicMock()
    invalidate_controller.try_invalidate_pr_environment()
    invalidate_controller._context._state_sync.invalidate_environment.assert_called_once_with(
        "hello_world_2"
    )

    no_invalidate_controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(invalidate_environment_after_deploy=False),
    )
    no_invalidate_controller._context._state_sync = mocker.MagicMock()
    no_invalidate_controller.try_invalidate_pr_environment()

    assert not no_invalidate_controller._context._state_sync.invalidate_environment.called


def test_try_merge_pr(github_client, make_controller, mocker: MockerFixture):
    no_merge_controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )
    no_merge_controller._pull_request = mocker.MagicMock()
    no_merge_controller.try_merge_pr()
    assert not no_merge_controller._pull_request.merge.called

    merge_controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.SQUASH),
    )
    merge_controller._pull_request = mocker.MagicMock()
    merge_controller.try_merge_pr()
    assert merge_controller._pull_request.method_calls == [
        call.merge(merge_method=MergeMethod.SQUASH)
    ]


bot_command_parsing_params = [
    (
        "deleted comment is invalid",
        "deleted",
        "/deploy",
        None,
        BotCommand.INVALID,
    ),
    (
        "deploy command without namespace is valid",
        "created",
        "/deploy",
        None,
        BotCommand.DEPLOY_PROD,
    ),
    (
        "deploy command with matching namespace is valid",
        "edited",
        "#SQLMesh/deploy",
        "#SQLMesh",
        BotCommand.DEPLOY_PROD,
    ),
    (
        "deploy command with non-matching namespace is invalid",
        "edited",
        "/deploy",
        "#SQLMesh",
        BotCommand.INVALID,
    ),
    (
        "non-deploy command with matching namespace is invalid",
        "edited",
        "#SQLMesh/blah",
        "#SQLMesh",
        BotCommand.INVALID,
    ),
    (
        "unknown command is invalid",
        "edited",
        "/blah",
        None,
        BotCommand.INVALID,
    ),
]


@pytest.mark.parametrize(
    "action, comment, namespace, command",
    [test[1:] for test in bot_command_parsing_params],
    ids=[test[0] for test in bot_command_parsing_params],
)
def test_bot_command_parsing(
    action, comment, namespace, command, github_client, make_controller, make_event_issue_comment
):
    controller = make_controller(
        make_event_issue_comment(action, comment),
        github_client,
        bot_config=GithubCICDBotConfig(
            command_namespace=namespace, enable_deploy_command=True, merge_method=MergeMethod.SQUASH
        ),
    )
    assert controller.get_command_from_comment() == command


def test_uncategorized(
    mocker,
    github_client,
    make_controller,
    make_snapshot,
    make_mock_check_run,
    make_mock_issue_comment,
    tmp_path: pathlib.Path,
):
    snapshot_categrozied = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_categrozied.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_uncategorized = make_snapshot(SqlModel(name="b", query=parse_one("select 1, ds")))
    mocker.patch(
        "sqlmesh.core.plan.Plan.modified_snapshots",
        PropertyMock(
            return_value={
                snapshot_categrozied.snapshot_id: snapshot_categrozied,
                snapshot_uncategorized.snapshot_id: snapshot_uncategorized,
            },
        ),
    )
    mock_repo = github_client.get_repo()
    mock_repo.create_check_run = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments = []
    mock_issue = mock_repo.get_issue()
    mock_issue.create_comment = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(
            comment=comment, created_comments=created_comments
        )
    )
    mock_issue.get_comments = mocker.MagicMock(side_effect=lambda: created_comments)
    controller = make_controller(
        "tests/fixtures/github/pull_request_synchronized.json", github_client
    )

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        controller.update_pr_environment_check(GithubCheckStatus.COMPLETED)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_environment_check_run = controller._check_run_mapping[
        "SQLMesh - PR Environment Synced"
    ].all_kwargs
    assert len(pr_environment_check_run) == 1
    assert (
        pr_environment_check_run[0]["output"]["summary"]
        == """<table><thead><tr><th colspan="3">PR Environment Summary</th></tr><tr><th>Model</th><th>Change Type</th><th>Dates Loaded</th></tr></thead><tbody><tr><td>a</td><td>Breaking</td><td>N/A</td></tr><tr><td>b</td><td>Uncategorized</td><td>N/A</td></tr></tbody></table>"""
    )
