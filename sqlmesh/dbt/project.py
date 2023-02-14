from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.common import project_config_path
from sqlmesh.dbt.macros import MacroConfig
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.package import PackageLoader, ProjectConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load as yaml_load


class Project:
    """Configuration for a DBT project"""

    def __init__(
        self,
        project_root: Path,
        project_name: str,
        profile: Profile,
        models: t.Dict[str, ModelConfig],
        sources: t.Dict[str, SourceConfig],
        seeds: t.Dict[str, SeedConfig],
        macros: UniqueKeyDict[str, MacroConfig],
        variables: t.Dict[str, t.Any],
        config_paths: t.Set[Path],
    ):
        """
        Args:
            project_root: Path to the root directory of the DBT project
            project_name: The name of the DBT project within the project file yaml
            profile: The profile associated with the project
            models: Dict of model name to model config for all models within this project
            sources: Dict of source name to source config for all sources within this project
            seeds: Dict of seed name to seed config for all seeds within this project
            macros: Dict of macro name to macro config for all macros within this project
            variables: Dict of variable name to variable value for all variables within this project
            config_paths: Paths to all the config files used by the project
        """
        self.project_root = project_root
        self.project_name = project_name
        self.profile = profile
        self.models = models
        self.sources = sources
        self.seeds = seeds
        self.macros = macros
        self.variables = variables
        self.config_paths = config_paths

    @classmethod
    def load(
        cls, project_root: t.Optional[Path] = None, target_name: t.Optional[str] = None
    ) -> Project:
        """
        Loads the configuration for the specified DBT project

        Args:
            project_root: Path to the root directory of the DBT project.
                          Defaults to the current directory if not specified.
            target_name: Name of the profile to use. Defaults to the project's default target name.

        Returns:
            Project instance for the specified DBT project
        """
        project_root = project_root or Path()
        project_file_path = project_config_path(project_root)
        project_yaml = yaml_load(project_file_path)

        project_name = project_yaml.get("name")
        if not project_name:
            raise ConfigError(f"{project_file_path.stem} must include project name.")

        profile = Profile.load(project_root, project_name)
        target = (
            profile.targets[target_name] if target_name else profile.targets[profile.default_target]
        )

        loader = PackageLoader()
        package = loader.load(project_root, target.schema_, ProjectConfig())

        return Project(
            project_root,
            project_name,
            profile,
            package.models,
            package.sources,
            package.seeds,
            package.macros,
            package.files,
        )

    @property
    def project_files(self) -> t.Set[Path]:
        paths = self.config_paths.copy()
        paths.add(self.profile.path)

        return paths
