# type: ignore
from unittest.mock import call
from unittest.result import TestResult

from pytest_mock.plugin import MockerFixture

from sqlmesh.core.user import User
from sqlmesh.integrations.github.cicd import command
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    GithubController,
    MergeMethod,
)
from sqlmesh.utils.errors import PlanError

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]


def get_mocked_controller(
    controller: GithubController,
    mocker: MockerFixture,
    test_success: bool = True,
    has_approval: bool = True,
    has_approvers: bool = True,
    success_update_pr_environment: bool = True,
    success_deploy_to_prod: bool = True,
) -> GithubController:
    controller.run_tests = mocker.MagicMock()
    if test_success:
        test, result = TestResult(), ""
        controller.run_tests.return_value = (TestResult(), "")
    else:
        test, result = TestResult(), "Failure"
        test.errors = [(None, None)]
    controller.run_tests.return_value = (test, result)
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController._required_approvers",
        [User(username="test", github_username="test_github")] if has_approvers else [],
    )
    controller._approvers = {"test_github"} if has_approval else set()
    update_check_mock = mocker.MagicMock()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController._update_check", update_check_mock
    )
    update_pr_environment_mock = mocker.MagicMock()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController.update_pr_environment",
        update_pr_environment_mock,
    )
    if not success_update_pr_environment:
        update_pr_environment_mock.side_effect = PlanError("Failed to update PR environment")
    update_comment_info_mock = mocker.MagicMock()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController.update_sqlmesh_comment_info",
        update_comment_info_mock,
    )
    deploy_to_prod_mocker = mocker.MagicMock()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController.deploy_to_prod",
        deploy_to_prod_mocker,
    )
    if not success_deploy_to_prod:
        deploy_to_prod_mocker.side_effect = PlanError("Failed to deploy to prod")
    merge_mock = mocker.MagicMock()
    mocker.patch("sqlmesh.integrations.github.cicd.command.GithubController.merge_pr", merge_mock)
    delete_mock = mocker.MagicMock()
    mocker.patch(
        "sqlmesh.integrations.github.cicd.command.GithubController.delete_pr_environment",
        delete_mock,
    )
    return controller


def test_run_all_success_with_approvers_approved(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(github_pr_synchronized_approvers_controller, mocker)
    command._run_all(controller, merge_method=None, delete=False)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
    ]
    assert controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_run_all_success_no_approvers(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(
        github_pr_synchronized_approvers_controller, mocker, has_approvers=False
    )
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
    ]
    assert not controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_run_all_success_with_approvers_approved_merge_delete(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(github_pr_synchronized_approvers_controller, mocker)
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
    ]
    assert controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert controller.merge_pr.call_args_list == [call(merge_method=MergeMethod.REBASE)]
    assert controller.delete_pr_environment.called


def test_run_all_success_with_approvers_none_approved(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(
        github_pr_synchronized_approvers_controller, mocker, has_approval=False
    )
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.NEUTRAL,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
    ]
    assert not controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_run_all_test_failed(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(
        github_pr_synchronized_approvers_controller, mocker, test_success=False
    )
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.FAILURE),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
    ]
    assert not controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_pr_update_failure(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(
        github_pr_synchronized_approvers_controller, mocker, success_update_pr_environment=False
    )
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.ACTION_REQUIRED,
        ),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
    ]
    assert not controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert not controller.update_sqlmesh_comment_info.called
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_deploy_to_prod_failure(
    github_pr_synchronized_approvers_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(
        github_pr_synchronized_approvers_controller, mocker, success_deploy_to_prod=False
    )
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Has Required Approval", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.ACTION_REQUIRED,
        ),
    ]
    assert controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_comment_command_invalid(
    github_pr_invalid_command_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(github_pr_invalid_command_controller, mocker)
    command._run_all(controller, merge_method=MergeMethod.REBASE, delete=True)
    assert not controller._update_check.called
    assert not controller.deploy_to_prod.called
    assert not controller.update_pr_environment.called
    assert not controller.update_sqlmesh_comment_info.called
    assert not controller.merge_pr.called
    assert not controller.delete_pr_environment.called


def test_comment_command_deploy_prod(
    github_pr_command_deploy_prod_controller: GithubController, mocker: MockerFixture
):
    controller = get_mocked_controller(github_pr_command_deploy_prod_controller, mocker)
    command._run_all(controller, merge_method=MergeMethod.SQUASH, delete=True)
    assert [
        (x[1]["name"], x[1]["status"], x[1]["conclusion"])
        for x in controller._update_check.call_args_list
    ] == [
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.QUEUED, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.IN_PROGRESS, None),
        ("SQLMesh - Run Unit Tests", GithubCheckStatus.COMPLETED, GithubCheckConclusion.SUCCESS),
        (
            "SQLMesh - Has Required Approval",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SKIPPED,
        ),
        ("SQLMesh - PR Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - PR Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
        ("SQLMesh - Prod Environment Synced", GithubCheckStatus.IN_PROGRESS, None),
        (
            "SQLMesh - Prod Environment Synced",
            GithubCheckStatus.COMPLETED,
            GithubCheckConclusion.SUCCESS,
        ),
    ]
    assert controller.deploy_to_prod.called
    assert controller.update_pr_environment.called
    assert controller.update_sqlmesh_comment_info.call_args_list == [
        call(
            value="- PR Virtual Data Environment: hello_world_2",
            find_regex="- PR Virtual Data Environment: .*",
            replace_if_exists=False,
        ),
    ]
    assert controller.merge_pr.call_args_list == [call(merge_method=MergeMethod.SQUASH)]
    assert controller.delete_pr_environment.called
