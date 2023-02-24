from __future__ import annotations

import typing as t

from sqlmesh.core.model.definition import BUILTIN_METHODS as MODEL_BUILTIN_METHODS
from sqlmesh.dbt.common import Dependencies
from sqlmesh.utils.metaprogramming import Executable
from sqlmesh.utils.pydantic import PydanticModel


class MacroConfig(PydanticModel):
    """Container class for macro configuration"""

    macro: Executable
    dependencies: Dependencies = Dependencies()


BUILTIN_METHODS: t.Dict[str, Executable] = {
    "is_incremental": Executable(
        payload="def is_incremental(): return False",
    ),
    **MODEL_BUILTIN_METHODS,
}


BUILTIN_METHOD_NAMES: t.Set[str] = {"source", "config", "ref", "var", *BUILTIN_METHODS}


def source_method(sources: t.Set[str], mapping: t.Dict[str, str]) -> Executable:
    """Create a source method that only includes the sources specified by the caller."""

    def source_map() -> str:
        deps = ", ".join(f"'{dep}': '{mapping[dep]}'" for dep in sorted(sources))
        return f"{{{deps}}}"

    return Executable(
        payload=f"""def source(source_name, table_name):
    return {source_map()}[".".join([source_name, table_name])]
""",
    )


def ref_method(refs: t.Set[str], mapping: t.Dict[str, str]) -> Executable:
    """Create a ref method that only includes the refs specified by the caller."""

    def ref_map() -> str:
        deps = ", ".join(f"'{dep}': '{mapping[dep]}'" for dep in sorted(refs))
        return f"{{{deps}}}"

    return Executable(
        payload=f"""def ref(package_name, model_name=None):
    if model_name:
        raise Exception("Package not supported.")
    model_name = package_name
    return {ref_map()}[model_name]
""",
    )


def var_method(variables: t.Set[str], mapping: t.Dict[str, t.Any]) -> Executable:
    """Create a var method that only includes the variables specified by the caller."""

    mapping = {k: f"'{v}'" if isinstance(v, str) else v for k, v in mapping.items()}

    def variable_map() -> str:
        vars_ = ", ".join(f"'{var}': {mapping[var]}" for var in sorted(variables) if var in mapping)
        return f"{{{vars_}}}"

    return Executable(
        payload=f"""def var(key, default=None):
    return {variable_map()}.get(key, default)
""",
    )


def config_method() -> Executable:
    return Executable(
        payload="""def config(*args, **kwargs):
    return ""
""",
    )
