import os

import click

path = click.option(
    "--path",
    default=os.getcwd(),
    help="Path to the models directory.",
)

config = click.option(
    "--config",
    help="Name of the config object.",
)

start_time = click.option(
    "-s",
    "--start",
    required=False,
    help="The start datetime of the interval this command will be applied for.",
)

end_time = click.option(
    "-e",
    "--end",
    required=False,
    help="The end datetime of the interval this command will be applied for.",
)

latest_time = click.option(
    "-l",
    "--latest",
    help="The latest time used for non incremental datasets (defaults to yesterday).",
)

expand = click.option(
    "--expand",
    multiple=True,
    help="Whether or not to expand materialized models (defaults to False). If True, all referenced models are expanded as raw queries. Multiple model names can also be specified, in which case only they will be expanded as raw queries.",
)

environment = click.option(
    "--environment",
    help="The environment to diff the current context against.",
)

file = click.option(
    "--file",
    help="The file to write the dag image to.",
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
