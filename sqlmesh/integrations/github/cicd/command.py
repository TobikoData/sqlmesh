from __future__ import annotations

import logging
import traceback

import click

from sqlmesh.core.analytics import cli_analytics
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
    GithubController,
    TestFailure,
)
from sqlmesh.utils.errors import CICDBotError, PlanError

logger = logging.getLogger(__name__)


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
@cli_analytics
def check_required_approvers(ctx: click.Context) -> None:
    """Checks if a required approver has provided approval on the PR."""
    _check_required_approvers(ctx.obj["github"])


def _run_tests(controller: GithubController) -> bool:
    controller.update_test_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        result, output = controller.run_tests()
        controller.update_test_check(
            status=GithubCheckStatus.COMPLETED,
            # Conclusion will be updated with final status based on test results
            conclusion=GithubCheckConclusion.NEUTRAL,
            result=result,
            output=output,
        )
        return result.wasSuccessful()
    except Exception:
        controller.update_test_check(
            status=GithubCheckStatus.COMPLETED,
            conclusion=GithubCheckConclusion.FAILURE,
            output=traceback.format_exc(),
        )
        return False


@github.command()
@click.pass_context
@cli_analytics
def run_tests(ctx: click.Context) -> None:
    """Runs the unit tests"""
    _run_tests(ctx.obj["github"])


def _update_pr_environment(controller: GithubController) -> bool:
    controller.update_pr_environment_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        controller.update_pr_environment()
        conclusion = controller.update_pr_environment_check(status=GithubCheckStatus.COMPLETED)
        return conclusion is not None and conclusion.is_success
    except Exception as e:
        conclusion = controller.update_pr_environment_check(
            status=GithubCheckStatus.COMPLETED, exception=e
        )
        return (
            conclusion is not None
            and not conclusion.is_failure
            and not conclusion.is_action_required
        )


@github.command()
@click.pass_context
@cli_analytics
def update_pr_environment(ctx: click.Context) -> None:
    """Creates or updates the PR environments"""
    _update_pr_environment(ctx.obj["github"])


def _gen_prod_plan(controller: GithubController) -> bool:
    controller.update_prod_plan_preview_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        plan_summary = controller.get_plan_summary(controller.prod_plan)
        controller.update_prod_plan_preview_check(
            status=GithubCheckStatus.COMPLETED,
            conclusion=GithubCheckConclusion.SUCCESS,
            summary=plan_summary,
        )
        return bool(plan_summary)
    except Exception as e:
        controller.update_prod_plan_preview_check(
            status=GithubCheckStatus.COMPLETED,
            conclusion=GithubCheckConclusion.FAILURE,
            summary=str(e),
        )
        return False


@github.command()
@click.pass_context
@cli_analytics
def gen_prod_plan(ctx: click.Context) -> None:
    """Generates the production plan"""
    controller = ctx.obj["github"]
    controller.update_prod_plan_preview_check(status=GithubCheckStatus.IN_PROGRESS)
    _gen_prod_plan(controller)


def _deploy_production(controller: GithubController) -> bool:
    controller.update_prod_environment_check(status=GithubCheckStatus.IN_PROGRESS)
    try:
        controller.deploy_to_prod()
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SUCCESS
        )
        controller.try_merge_pr()
        controller.try_invalidate_pr_environment()
        return True
    except PlanError:
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.ACTION_REQUIRED
        )
        return False


@github.command()
@click.pass_context
@cli_analytics
def deploy_production(ctx: click.Context) -> None:
    """Deploys the production environment"""
    _deploy_production(ctx.obj["github"])


def _run_all(controller: GithubController) -> None:
    has_required_approval = False
    is_auto_deploying_prod = (
        controller.deploy_command_enabled or controller.do_required_approval_check
    )
    if controller.is_comment_added:
        if not controller.deploy_command_enabled:
            # We aren't using commands so we can just return
            return
        command = controller.get_command_from_comment()
        if command.is_invalid:
            # Probably a comment unrelated to SQLMesh so we do nothing
            return
        elif command.is_deploy_prod:
            has_required_approval = True
        else:
            raise CICDBotError(f"Unsupported command: {command}")
    controller.update_pr_environment_check(status=GithubCheckStatus.QUEUED)
    controller.update_prod_plan_preview_check(status=GithubCheckStatus.QUEUED)
    controller.update_test_check(status=GithubCheckStatus.QUEUED)
    if is_auto_deploying_prod:
        controller.update_prod_environment_check(status=GithubCheckStatus.QUEUED)
    tests_passed = _run_tests(controller)
    if controller.do_required_approval_check:
        if has_required_approval:
            controller.update_required_approval_check(
                status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SKIPPED
            )
        else:
            controller.update_required_approval_check(status=GithubCheckStatus.QUEUED)
            has_required_approval = _check_required_approvers(controller)
    if not tests_passed:
        controller.update_pr_environment_check(
            status=GithubCheckStatus.COMPLETED,
            exception=TestFailure(),
        )
        controller.update_prod_plan_preview_check(
            status=GithubCheckStatus.COMPLETED,
            conclusion=GithubCheckConclusion.SKIPPED,
            summary="Unit Test(s) Failed so skipping creating prod plan",
        )
        if is_auto_deploying_prod:
            controller.update_prod_environment_check(
                status=GithubCheckStatus.COMPLETED,
                conclusion=GithubCheckConclusion.SKIPPED,
                skip_reason="Unit Test(s) Failed so skipping deploying to production",
            )
        return
    pr_environment_updated = _update_pr_environment(controller)
    prod_plan_generated = False
    if pr_environment_updated:
        prod_plan_generated = _gen_prod_plan(controller)
    else:
        controller.update_prod_plan_preview_check(
            status=GithubCheckStatus.COMPLETED, conclusion=GithubCheckConclusion.SKIPPED
        )
    if tests_passed and has_required_approval and pr_environment_updated and prod_plan_generated:
        _deploy_production(controller)
    elif is_auto_deploying_prod:
        if not has_required_approval:
            skip_reason = (
                "Skipped Deploying to Production because a required approver has not approved"
            )
        elif not pr_environment_updated:
            skip_reason = (
                "Skipped Deploying to Production because the PR environment was not updated"
            )
        elif not prod_plan_generated:
            skip_reason = (
                "Skipped Deploying to Production because the production plan could not be generated"
            )
        else:
            skip_reason = "Skipped Deploying to Production for an unknown reason"
        controller.update_prod_environment_check(
            status=GithubCheckStatus.COMPLETED,
            conclusion=GithubCheckConclusion.SKIPPED,
            skip_reason=skip_reason,
        )


@github.command()
@click.pass_context
@cli_analytics
def run_all(ctx: click.Context) -> None:
    """Runs all the commands in the correct order."""
    return _run_all(ctx.obj["github"])
