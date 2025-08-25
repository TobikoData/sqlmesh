import typing as t
import click
from click.core import Context, Parameter


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
