from __future__ import annotations

import logging
import typing as t
from pathlib import Path

from sqlmesh.dbt.common import PROJECT_FILENAME, Dependencies, load_yaml
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroInfo
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


logger = logging.getLogger(__name__)


class MacroConfig(PydanticModel):
    """Class to contain macro configuration"""

    info: MacroInfo
    dependencies: Dependencies
    path: Path


class Package(PydanticModel):
    """Class to contain package configuration"""

    name: str
    tests: t.Dict[str, TestConfig]
    sources: t.Dict[str, SourceConfig]
    seeds: t.Dict[str, SeedConfig]
    models: t.Dict[str, ModelConfig]
    variables: t.Dict[str, t.Any]
    macros: t.Dict[str, MacroConfig]
    files: t.Set[Path]

    @property
    def macro_infos(self) -> t.Dict[str, MacroInfo]:
        return {name: macro.info for name, macro in self.macros.items()}


class PackageLoader:
    """Loader for DBT packages"""

    def __init__(self, context: DbtContext):
        self._context = context

    def load(self, package_root: Path) -> Package:
        """
        Loads the specified package.

        Returns:
            Package containing the configuration found within this package
        """
        logger.debug("Loading package at '%s'.", package_root)
        project_file_path = package_root / PROJECT_FILENAME
        logger.debug("Processing project file '%s'.", project_file_path)
        if not project_file_path.exists():
            raise ConfigError(f"Could not find {PROJECT_FILENAME} in '{package_root}'.")

        project_yaml = load_yaml(project_file_path)
        package_name = self._context.render(project_yaml.get("name", ""))
        if not package_name:
            raise ConfigError(f"Package '{package_root}' must include package name.")

        # Only include globally-scoped variables (i.e. filter out the package-scoped ones)
        logger.debug("Processing project variables.")

        all_variables = project_yaml.get("vars") or {}
        all_variables.update(all_variables.pop(package_name, None) or {})

        package_variables = {
            var: value for var, value in all_variables.items() if not isinstance(value, dict)
        }

        tests = _fix_paths(self._context.manifest.tests(package_name), package_root)
        models = _fix_paths(self._context.manifest.models(package_name), package_root)
        seeds = _fix_paths(self._context.manifest.seeds(package_name), package_root)
        macros = _fix_paths(self._context.manifest.macros(package_name), package_root)
        sources = self._context.manifest.sources(package_name)

        config_paths = {
            config.path.absolute()  # type: ignore
            for configs in [models.values(), seeds.values(), macros.values()]
            for config in configs
        }
        for path in package_root.glob("**/*.y*ml"):
            config_paths.add(path.absolute())

        return Package(
            name=package_name,
            tests=tests,
            models=models,
            sources=sources,
            seeds=seeds,
            variables=package_variables,
            macros=macros,
            files=config_paths,
        )


T = t.TypeVar("T", TestConfig, ModelConfig, MacroConfig, SeedConfig)


def _fix_paths(configs: t.Dict[str, T], package_root: Path) -> t.Dict[str, T]:
    return {
        name: config.copy(update={"path": package_root / config.path})
        for name, config in configs.items()
    }
