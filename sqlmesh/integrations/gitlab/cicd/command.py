import click

from sqlmesh.cli.main import cli
from pathlib import Path

from sqlmesh.integrations.gitlab.cicd.controller import (
    GitlabController,
    run_all_checks,
    run_deploy_command,
)


@cli.group(name="gitlab")
@click.option("--token", required=True, help="GitLab private token.", envvar="GITLAB_TOKEN")
@click.pass_context
def gitlab(ctx: click.Context, token: str) -> None:
    """Commands for GitLab CI/CD integration."""
    ctx.obj["gitlab"] = GitlabController(paths=[Path(".")], token=token)


@gitlab.command(name="run-all-checks")
@click.pass_context
def run_all_checks_command(ctx: click.Context) -> None:
    """Runs all checks for a GitLab merge request."""
    run_all_checks(ctx.obj["gitlab"])


@gitlab.command(name="run-deploy-command")
@click.pass_context
def run_deploy_command_command(ctx: click.Context) -> None:
    """Runs the deploy command for a GitLab merge request."""
    controller = ctx.obj["gitlab"]
    command = controller.get_command_from_comment()
    if command.is_deploy_prod:
        run_deploy_command(controller)
