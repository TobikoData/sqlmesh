from __future__ import annotations

import typing as t

import click

from sqlmesh.cli import error_handler
from sqlmesh.cli import options as opt
from sqlmesh.integrations.github.cicd.command import github


@click.group(no_args_is_help=True)
@opt.paths
@opt.config
@click.pass_context
@error_handler
def bot(
    ctx: click.Context,
    paths: t.List[str],
    config: t.Optional[str] = None,
) -> None:
    """SQLMesh CI/CD Bot. Currently only Github Actions is supported. See https://sqlmesh.readthedocs.io/en/stable/integrations/github/ for details"""
    ctx.obj = {
        "paths": paths,
        "config": config,
    }


bot.add_command(github)


if __name__ == "__main__":
    bot()
