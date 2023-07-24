# type: ignore
import typing as t
from unittest.mock import Mock, call

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import constants as c
from sqlmesh.integrations.github.cicd.controller import (
    BotCommand,
    GithubController,
    GithubEvent,
    MergeMethod,
)
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import CICDBotError
from tests.integrations.github.cicd.helper import get_mocked_controller

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]


def test_github_controller_approvers(github_pr_synchronized_approvers_controller: GithubController):
    controller = github_pr_synchronized_approvers_controller
    assert not controller.has_required_approval
    assert controller.do_required_approval_check


def test_github_controller_pr_plan(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = github_pr_synchronized_approvers_controller
    mocked_apply = mocker.MagicMock()
    mocker.patch("sqlmesh.core.context.Context.apply", mocked_apply)
    pr_plan = controller.pr_plan
    assert pr_plan.environment_name == "hello_world_2"
    assert pr_plan.skip_backfill
    assert not pr_plan.auto_categorization_enabled
    assert not pr_plan.no_gaps
    assert not mocked_apply.called
    assert not controller._console.method_calls
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)


def test_github_controller_prod_plan(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = github_pr_synchronized_approvers_controller
    mocked_apply = mocker.MagicMock()
    mocker.patch("sqlmesh.core.context.Context.apply", mocked_apply)
    prod_plan = controller.prod_plan
    assert prod_plan.environment_name == c.PROD
    assert not prod_plan.skip_backfill
    assert not prod_plan.auto_categorization_enabled
    assert prod_plan.no_gaps
    assert not mocked_apply.called
    assert not controller._console.method_calls
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)


def test_github_controller_prod_plan_with_gaps(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = github_pr_synchronized_approvers_controller
    mocked_apply = mocker.MagicMock()
    mocker.patch("sqlmesh.core.context.Context.apply", mocked_apply)
    prod_plan_with_gaps = controller.prod_plan_with_gaps
    assert prod_plan_with_gaps.environment_name == c.PROD
    assert not prod_plan_with_gaps.skip_backfill
    assert not prod_plan_with_gaps.auto_categorization_enabled
    assert not prod_plan_with_gaps.no_gaps
    assert not mocked_apply.called
    assert not controller._console.method_calls
    assert controller._context._run_plan_tests.call_args == call(skip_tests=True)


def test_run_tests(github_pr_synchronized_approvers_controller: GithubController):
    controller = github_pr_synchronized_approvers_controller
    controller.run_tests()
    assert controller._context._run_tests.called


def test_update_sqlmesh_comment_info(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    class CommentResponse(Mock):
        body: str = "- Test Header: blah"

        def edit(self, body):
            self.body = body

    controller = github_pr_synchronized_approvers_controller
    comment_response = CommentResponse()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.controller.GithubController._get_or_create_comment",
        comment_response,
    )
    resp = controller.update_sqlmesh_comment_info("test1", find_regex=None, replace_if_exists=False)
    assert resp.body == "- Test Header: blah\ntest1"
    resp = controller.update_sqlmesh_comment_info(
        "test2", find_regex="^- Test Header:.*", replace_if_exists=False
    )
    assert resp.body == "- Test Header: blah\ntest1"
    resp = controller.update_sqlmesh_comment_info(
        "test3", find_regex="^- Test Header:.*", replace_if_exists=True
    )
    assert resp.body == "test3\ntest1"


def test_deploy_to_prod_merge_error(github_pr_synchronized_approvers_controller: GithubController):
    controller = github_pr_synchronized_approvers_controller
    controller._pull_request = AttributeDict({"merged": True})
    with pytest.raises(
        CICDBotError,
        match=r"^PR is already merged and this event was triggered prior to the merge.$",
    ):
        controller.deploy_to_prod()


def test_delete_pr_environment(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = github_pr_synchronized_approvers_controller
    controller._context._state_sync = mock_state_sync = mocker.MagicMock()
    controller.delete_pr_environment()
    mock_state_sync.invalidate_environment.assert_called_once_with("hello_world_2")


def test_merge_pr(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = github_pr_synchronized_approvers_controller
    controller._pull_request = mocker.MagicMock()
    controller.merge_pr(merge_method=MergeMethod.MERGE)
    controller._pull_request.method_calls == [call.merge(merge_method=MergeMethod.MERGE)]


def test_bot_command_parsing(
    github_pull_request_comment_raw: t.Dict[str, t.Any], mocker: MockerFixture
):
    comment_raw = github_pull_request_comment_raw
    controller = get_mocked_controller(GithubEvent.from_obj(comment_raw), mocker)
    assert controller.is_comment_triggered
    assert controller.get_command_from_comment() == BotCommand.INVALID
    comment_raw["comment"]["body"] = "/deploy"
    controller = get_mocked_controller(GithubEvent.from_obj(comment_raw), mocker)
    assert controller.get_command_from_comment() == BotCommand.DEPLOY_PROD
    assert controller.get_command_from_comment("#SQLMesh") == BotCommand.INVALID
    comment_raw["comment"]["body"] = "#SQLMesh/deploy"
    controller = get_mocked_controller(GithubEvent.from_obj(comment_raw), mocker)
    assert controller.get_command_from_comment() == BotCommand.INVALID
    assert controller.get_command_from_comment("#SQLMesh") == BotCommand.DEPLOY_PROD
    comment_raw["comment"]["body"] = "Something Something /deploy"
    controller = get_mocked_controller(GithubEvent.from_obj(comment_raw), mocker)
    assert controller.get_command_from_comment() == BotCommand.INVALID
    assert controller.get_command_from_comment() == BotCommand.INVALID
    comment_raw["comment"][
        "body"
    ] = """
    /deploy
    """
    controller = get_mocked_controller(GithubEvent.from_obj(comment_raw), mocker)
    assert controller.get_command_from_comment() == BotCommand.DEPLOY_PROD
