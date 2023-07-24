from __future__ import annotations

import logging
import typing as t

import click

from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    GithubController,
    MergeMethod,
)
from sqlmesh.utils.errors import CICDBotError, PlanError

logger = logging.getLogger(__name__)


merge_method_option = click.option(
    "--merge-method",
    type=click.Choice(MergeMethod),  # type: ignore
    help="Enables merging PR after successfully deploying to production using the provided method.",
)

delete_option = click.option(
    "--delete",
    is_flag=True,
    help="Delete the PR environment after successfully deploying to production",
)


@click.group(no_args_is_help=True)
@click.option(
    "--token",
    type=str,
    help="The Github Token to be used. Pass in `${{ secrets.GITHUB_TOKEN }}` if you want to use the one created by Github actions",
)
@click.pass_context
def github(ctx: click.Context, token: str) -> None:
    """Github Action CI/CD Bot. See https://sqlmesh.readthedocs.io/en/stable/integrations/github/ for details"""
    ctx.obj["github"] = GithubController(
        paths=ctx.obj["paths"],
        token=token,
        config=ctx.obj["config"],
    )


def _check_required_approvers(controller: GithubController) -> bool:
    controller.update_required_approval_check(status=GithubCheckStatus.IN_PROGRESS)
    if controller.has_required_approval:
        controller.update_required_approval_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SUCCESS
        )
        return True
    controller.update_required_approval_check(
        status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.NEUTRAL
    )
    return False


@github.command()
@click.pass_context
def check_required_approvers(ctx: click.Context) -> None:
    """Checks if a required approver has provided approval on the PR."""
    _check_required_approvers(ctx.obj["github"])


def _run_tests(controller: GithubController) -> bool:
    controller.update_test_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        result, failed_output = controller.run_tests()
        controller.update_test_check(
            status=GithubCheckStatus.COMPLETED,
            # Conclusion will be updated with final status based on test results
            conclusion=GithubCheckConclusion.NEUTRAL,
            result=result,
            failed_output=failed_output,
        )
        return result.wasSuccessful()
    except Exception:
        controller.update_test_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.FAILURE
        )
        return False


@github.command()
@click.pass_context
def run_tests(ctx: click.Context) -> None:
    """Runs the unit tests"""
    _run_tests(ctx.obj["github"])


def _update_pr_environment(controller: GithubController) -> bool:
    controller.update_pr_environment_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        controller.update_pr_environment()
        controller.update_pr_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SUCCESS
        )
        return True
    except PlanError:
        controller.update_pr_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.ACTION_REQUIRED
        )
        return False


@github.command()
@click.pass_context
def update_pr_environment(ctx: click.Context) -> None:
    """Creates or updates the PR environments"""
    _update_pr_environment(ctx.obj["github"])


def _deploy_production(
    controller: GithubController,
    merge_method: t.Optional[MergeMethod],
    delete_environment_after_deploy: bool = True,
) -> bool:
    controller.update_prod_environment_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        controller.deploy_to_prod()
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SUCCESS
        )
        if merge_method:
            controller.merge_pr(merge_method=merge_method)
        if delete_environment_after_deploy:
            controller.delete_pr_environment()
        return True
    except PlanError:
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.ACTION_REQUIRED
        )
        return False


@github.command()
@merge_method_option
@delete_option
@click.pass_context
def deploy_production(
    ctx: click.Context, merge_method: t.Optional[MergeMethod], delete: bool
) -> None:
    """Deploys the production environment"""
    _deploy_production(
        ctx.obj["github"], merge_method=merge_method, delete_environment_after_deploy=delete
    )


def _run_all(
    controller: GithubController,
    merge_method: t.Optional[MergeMethod],
    delete: bool,
    command_namespace: t.Optional[str] = None,
) -> None:
    has_required_approval = False
    if controller.is_comment_triggered:
        command = controller.get_command_from_comment(command_namespace)
        if command.is_invalid:
            # Probably a comment unrelated to SQLMesh so we do nothing
            return
        elif command.is_deploy_prod:
            has_required_approval = True
        else:
            raise CICDBotError(f"Unsupported command: {command}")
    controller.update_pr_environment_check(status=GithubCheckStatus.QUEUED)
    controller.update_prod_environment_check(status=GithubCheckStatus.QUEUED)
    controller.update_test_check(status=GithubCheckStatus.QUEUED)
    tests_passed = _run_tests(controller)
    if not has_required_approval and controller.do_required_approval_check:
        controller.update_required_approval_check(status=GithubCheckStatus.QUEUED)
        has_required_approval = _check_required_approvers(controller)
    else:
        controller.update_required_approval_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SKIPPED
        )
    pr_environment_updated = _update_pr_environment(controller)
    if tests_passed and has_required_approval and pr_environment_updated:
        _deploy_production(
            controller, merge_method=merge_method, delete_environment_after_deploy=delete
        )
    else:
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SKIPPED
        )


@github.command()
@merge_method_option
@delete_option
@click.option(
    "--command_namespace",
    type=str,
    help="Namespace to use for SQLMesh commands. For example if you provide `#SQLMesh` as a value then commands will be expected in the format of `#SQLMesh/<command>`.",
)
@click.pass_context
def run_all(
    ctx: click.Context,
    merge_method: t.Optional[MergeMethod],
    delete: bool,
    command_namespace: t.Optional[str],
) -> None:
    """Runs all the commands in the correct order."""
    return _run_all(ctx.obj["github"], merge_method, delete, command_namespace)
