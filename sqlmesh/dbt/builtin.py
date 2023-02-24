from __future__ import annotations

import typing as t
from os import getenv


def builtin_jinja(variables: t.Optional[t.Dict[str, t.Any]] = None) -> t.Dict[str, t.Callable]:
    variables = variables or {}
    return {
        "env_var": env_var,
        "var": generate_var(variables),
    }


def env_var(name: str, default: t.Optional[str] = None) -> t.Optional[str]:
    return getenv(name, default)


def generate_var(variables: t.Dict[str, t.Any]) -> t.Callable:
    generated_var_mapping = variables.copy()

    def var(name: str, default: t.Optional[str] = None) -> str:
        return generated_var_mapping.get(name, default)

    return var
