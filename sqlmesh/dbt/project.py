from __future__ import annotations

import typing as t
import logging
from pathlib import Path

from sqlmesh.core.console import get_console
from sqlmesh.dbt.common import PROJECT_FILENAME, load_yaml
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.package import Package, PackageLoader
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.errors import ConfigError

logger = logging.getLogger(__name__)


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
    def load(cls, context: DbtContext, variables: t.Optional[t.Dict[str, t.Any]] = None) -> Project:
        """
        Loads the configuration for the specified DBT project

        Args:
            context: dbt context for this project
            variables: dbt variable overrides

        Returns:
            Project instance for the specified DBT project
        """
        context = context.copy()

        project_file_path = Path(context.project_root, PROJECT_FILENAME)
        logger.debug("Processing project file '%s'.", project_file_path)
        if not project_file_path.exists():
            raise ConfigError(f"Could not find {PROJECT_FILENAME} in {context.project_root}")
        project_yaml = load_yaml(project_file_path)

        project_name = context.render(project_yaml.get("name", ""))
        context.project_name = project_name
        if not context.project_name:
            raise ConfigError(f"{project_file_path.stem} must include project name.")

        profile_name = context.render(project_yaml.get("profile", "")) or context.project_name
        context.profile_name = profile_name

        profile = Profile.load(context, context.target_name)
        context.target = profile.target

        variable_overrides = variables or {}
        context.manifest = ManifestHelper(
            project_file_path.parent,
            profile.path.parent,
            profile_name,
            target=profile.target,
            variable_overrides=variable_overrides,
            cache_dir=context.sqlmesh_config.cache_dir,
            model_defaults=context.sqlmesh_config.model_defaults,
        )

        extra_fields = profile.target.extra
        if extra_fields:
            extra_str = ",".join(f"'{field}'" for field in extra_fields)
            get_console().log_warning(
                f"{profile.target.type} adapter does not currently support {extra_str}."
            )

        packages = {}
        package_loader = PackageLoader(context)

        packages[context.project_name] = package_loader.load(context.project_root)

        packages_dir = Path(
            context.render(project_yaml.get("packages-install-path", "dbt_packages"))
        )
        if not packages_dir.is_absolute():
            packages_dir = Path(context.project_root, packages_dir)

        for path in packages_dir.glob(f"*/{PROJECT_FILENAME}"):
            package = package_loader.load(path.parent)
            packages[package.name] = package

        # Variable resolution precedence:
        # 1. Variable overrides
        # 2. Package-scoped variables in the root project's dbt_project.yml
        # 3. Global project variables in the root project's dbt_project.yml
        # 4. Variables in the package's dbt_project.yml
        all_project_variables = {**(project_yaml.get("vars") or {}), **(variable_overrides or {})}
        for name, package in packages.items():
            if isinstance(all_project_variables.get(name), dict):
                project_vars_copy = all_project_variables.copy()
                package_scoped_vars = project_vars_copy.pop(name)
                package.variables.update(project_vars_copy)
                package.variables.update(package_scoped_vars)
            else:
                package.variables.update(all_project_variables)
            if variable_overrides:
                package.variables.update(variable_overrides)

        return Project(context, profile, packages)

    @property
    def project_files(self) -> t.Set[Path]:
        paths = {self.profile.path}
        for package in self.packages.values():
            paths.update(package.files)

        return paths
