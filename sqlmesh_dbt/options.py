import typing as t
import click
from click.core import Context, Parameter
from pathlib import Path


class YamlParamType(click.ParamType):
    name = "yaml"

    def convert(
        self, value: t.Any, param: t.Optional[Parameter], ctx: t.Optional[Context]
    ) -> t.Any:
        if not isinstance(value, str):
            self.fail(f"Input value '{value}' should be a string", param, ctx)

        from sqlmesh.utils import yaml

        try:
            parsed = yaml.load(source=value, render_jinja=False)
        except:
            self.fail(f"String '{value}' is not valid YAML", param, ctx)

        if not isinstance(parsed, dict):
            self.fail(f"String '{value}' did not evaluate to a dict, got: {parsed}", param, ctx)

        return parsed


vars_option = click.option(
    "--vars",
    type=YamlParamType(),
    help="Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file. This argument should be a YAML string, eg. '{my_variable: my_value}'",
)

select_option = click.option(
    "-s",
    "--select",
    multiple=True,
    help="Specify the nodes to include.",
)
model_option = click.option(
    "-m",
    "--models",
    "--model",
    multiple=True,
    help="Specify the model nodes to include; other nodes are excluded.",
)
exclude_option = click.option("--exclude", multiple=True, help="Specify the nodes to exclude.")

# TODO: expand this out into --resource-type/--resource-types and --exclude-resource-type/--exclude-resource-types
resource_types = [
    "metric",
    "semantic_model",
    "saved_query",
    "source",
    "analysis",
    "model",
    "test",
    "unit_test",
    "exposure",
    "snapshot",
    "seed",
    "default",
    "all",
]
resource_type_option = click.option(
    "--resource-type", type=click.Choice(resource_types, case_sensitive=False)
)


def global_options(fn: t.Callable) -> t.Callable:
    """
    These options can be specified either before or after a subcommand and will still Just Work™

    eg, the following are equivalent, which is *not* Click's default behaviour:

    $ sqlmesh_dbt --target foo list
    $ sqlmesh_dbt list --target foo
    """

    for option in [
        click.option("--profile", help="Which existing profile to load. Overrides output.profile"),
        click.option("-t", "--target", help="Which target to load for the given profile"),
        click.option(
            "-d",
            "--debug/--no-debug",
            help="Display debug logging during dbt execution. Useful for debugging and making bug reports events to help when debugging.",
        ),
        click.option(
            "--log-level",
            default="info",
            type=click.Choice(["debug", "info", "warn", "error", "none"]),
            help="Specify the minimum severity of events that are logged to the console and the log file.",
        ),
        click.option(
            "--profiles-dir",
            type=click.Path(exists=True, file_okay=False, path_type=Path),
            help="Which directory to look in for the profiles.yml file. If not set, dbt will look in the current working directory first, then HOME/.dbt/",
        ),
        click.option(
            "--project-dir",
            type=click.Path(exists=True, file_okay=False, path_type=Path),
            help="Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
        ),
    ]:
        option(fn)

    return fn


def run_options(fn: t.Callable) -> t.Callable:
    """Non-global options for `sqlmesh_dbt run`"""
    for option in [
        select_option,
        model_option,
        exclude_option,
        resource_type_option,
        click.option(
            "-f",
            "--full-refresh",
            is_flag=True,
            default=False,
            help="If specified, sqlmesh will drop incremental models and fully-recalculate the incremental table from the model definition.",
        ),
        click.option(
            "--env",
            "--environment",
            help="Run against a specific Virtual Data Environment (VDE) instead of the main environment",
        ),
        click.option(
            "--empty/--no-empty", default=False, help="If specified, limit input refs and sources"
        ),
        click.option(
            "--threads",
            type=int,
            help="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
        ),
        vars_option,
    ]:
        option(fn)

    return fn


def list_options(fn: t.Callable) -> t.Callable:
    """Non-global options for `sqlmesh_dbt list`"""
    for option in [
        select_option,
        model_option,
        exclude_option,
        resource_type_option,
        vars_option,
    ]:
        option(fn)

    return fn
