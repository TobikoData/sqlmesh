from __future__ import annotations

import os

import click

path = click.option(
    "--path",
    default=os.getcwd(),
    help="Path to the models directory.",
)

config = click.option(
    "--config",
    help="Name of the config object. Only applicable to configuration defined using Python script.",
)

start_time = click.option(
    "-s",
    "--start",
    required=False,
    help="The start datetime of the interval for which this command will be applied.",
)

end_time = click.option(
    "-e",
    "--end",
    required=False,
    help="The end datetime of the interval for which this command will be applied.",
)

latest_time = click.option(
    "-l",
    "--latest",
    help="The latest time used for non incremental datasets (defaults to now).",
)

expand = click.option(
    "--expand",
    multiple=True,
    help="Whether or not to expand materialized models (defaults to False). If True, all referenced models are expanded as raw queries. Multiple model names can also be specified, in which case only they will be expanded as raw queries.",
)

file = click.option(
    "--file",
    help="The file to which the dag image should be written.",
)

match_pattern = click.option(
    "-k",
    multiple=True,
    help="Only run tests that match the pattern of substring.",
)

verbose = click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Verbose output.",
)
