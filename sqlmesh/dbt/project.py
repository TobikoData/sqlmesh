from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.common import PROJECT_FILENAME
from sqlmesh.dbt.package import Package, PackageLoader, ProjectConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load as yaml_load


class Project:
    """Configuration for a DBT project"""

    def __init__(
        self,
        project_root: Path,
        project_name: str,
        profile: Profile,
        packages: t.Dict[str, Package],
    ):
        """
        Args:
            project_root: Path to the root directory of the DBT project
            project_name: The name of the DBT project within the project file yaml
            profile: The profile associated with the project
            packages: The packages in this project. The project should be included
                      with the project name as the key
        """
        self.project_root = project_root
        self.project_name = project_name
        self.profile = profile
        self.packages = packages

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
        project_file_path = Path(project_root, PROJECT_FILENAME)
        if not project_file_path.exists():
            raise ConfigError(f"Could not find {PROJECT_FILENAME} in {project_root}")
        project_yaml = yaml_load(project_file_path)

        project_name = project_yaml.get("name")
        if not project_name:
            raise ConfigError(f"{project_file_path.stem} must include project name.")

        profile = Profile.load(project_root, project_name)
        target = (
            profile.targets[target_name] if target_name else profile.targets[profile.default_target]
        )

        packages = {}
        loader = PackageLoader()

        packages[project_name] = loader.load(project_root, target.schema_, ProjectConfig())
        project_config = loader.project_config

        packages_dir = Path(project_root, "dbt_packages")
        for path in packages_dir.glob(f"**/{PROJECT_FILENAME}"):
            name = yaml_load(path).get("name")
            if not name:
                raise ConfigError(f"{path} must include package name")

            packages[name] = loader.load(
                path.parent, target.schema_, cls._overrides_for_package(name, project_config)
            )

        return Project(project_root, project_name, profile, packages)

    @classmethod
    def _overrides_for_package(cls, name: str, config: ProjectConfig) -> ProjectConfig:
        overrides = ProjectConfig()

        source_overrides = {
            scope[1:]: value
            for scope, value in config.source_config.items()
            if scope and scope[0] == name
        }
        if source_overrides:
            overrides.source_config = source_overrides

        seed_overrides = {
            scope[1:]: value
            for scope, value in config.seed_config.items()
            if scope and scope[0] == name
        }
        if seed_overrides:
            overrides.seed_config = seed_overrides

        model_overrides = {
            scope[1:]: value
            for scope, value in config.model_config.items()
            if scope and scope[0] == name
        }
        if model_overrides:
            overrides.model_config = model_overrides

        return overrides

    @property
    def project_files(self) -> t.Set[Path]:
        paths = {self.profile.path}
        for package in self.packages.values():
            paths.update(package.files)

        return paths
