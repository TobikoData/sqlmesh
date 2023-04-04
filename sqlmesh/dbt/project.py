from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.common import PROJECT_FILENAME, DbtContext, load_yaml
from sqlmesh.dbt.package import Package, PackageLoader, ProjectConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.errors import ConfigError


class Project:
    """Configuration for a DBT project"""

    def __init__(
        self,
        context: DbtContext,
        profile: Profile,
        packages: t.Dict[str, Package],
    ):
        """
        Args:
            context: DBT context for the project
            profile: The profile associated with the project
            packages: The packages in this project. The project should be included
                      with the project name as the key
        """
        self.context = context
        self.profile = profile
        self.packages = packages

    @classmethod
    def load(cls, context: DbtContext) -> Project:
        """
        Loads the configuration for the specified DBT project

        Args:
            context: DBT context for this project

        Returns:
            Project instance for the specified DBT project
        """
        context = context.copy()

        project_file_path = Path(context.project_root, PROJECT_FILENAME)
        if not project_file_path.exists():
            raise ConfigError(f"Could not find {PROJECT_FILENAME} in {context.project_root}")
        project_yaml = load_yaml(project_file_path)

        variables = project_yaml.get("vars", {})
        context.variables = {
            name: var for name, var in variables.items() if not isinstance(var, t.Dict)
        }

        context.project_name = context.render(project_yaml.get("name", ""))
        if not context.project_name:
            raise ConfigError(f"{project_file_path.stem} must include project name.")

        context.profile_name = (
            context.render(project_yaml.get("profile", "")) or context.project_name
        )

        profile = Profile.load(context, context.target_name)
        context.target = profile.target

        packages = {}
        root_loader = PackageLoader(context, ProjectConfig())

        packages[context.project_name] = root_loader.load()
        project_config = root_loader.project_config

        packages_dir = Path(
            context.render(project_yaml.get("packages-install-path", "dbt_packages"))
        )
        if not packages_dir.is_absolute():
            packages_dir = Path(context.project_root, packages_dir)

        for path in packages_dir.glob(f"*/{PROJECT_FILENAME}"):
            name = context.render(load_yaml(path).get("name", ""))
            if not name:
                raise ConfigError(f"{path} must include package name")

            package_context = context.copy()
            package_context.project_root = path.parent
            package_context.variables = {}
            packages[name] = PackageLoader(
                package_context, cls._overrides_for_package(name, project_config)
            ).load()

        for name, package in packages.items():
            package_vars = variables.get(name)

            if isinstance(package_vars, dict):
                package.variables.update(package_vars)

        return Project(context, profile, packages)

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
