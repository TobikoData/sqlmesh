from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.config.connection import ConnectionConfig
from sqlmesh.dbt.common import PROJECT_FILENAME, DbtContext
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils.errors import ConfigError


class Profile:
    """
    A class to read DBT profiles and obtain the project's target data warehouse configuration
    """

    PROFILE_FILE = "profiles.yml"

    def __init__(
        self,
        path: Path,
        targets: t.Dict[str, TargetConfig],
        default_target: str,
    ):
        """
        Args:
            path: Path to the profile file
            targets: Dict of targets defined for the project
            default_target: Name of the default target for the proejct
        """
        self.path = path
        self.targets = targets
        self.default_target = default_target

    @classmethod
    def load(cls, context: DbtContext) -> Profile:
        """
        Loads the profile for the specified project

        Args:
            context: DBT context for this profile

        Returns:
            The Profile for the specified project
        """
        if not context.project_name:
            project_file = Path(context.project_root, PROJECT_FILENAME)
            if not project_file.exists():
                raise ConfigError(f"Could not find {PROJECT_FILENAME} in {context.project_root}")

            context.project_name = context.render(context.load_yaml(project_file).get("name", ""))
            if not context.project_name:
                raise ConfigError(f"{project_file.stem} must include project name.")

        profile_filepath = cls._find_profile(context.project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found.")

        targets, default_target = cls._read_profile(profile_filepath, context)
        return Profile(profile_filepath, targets, default_target)

    @classmethod
    def _find_profile(cls, project_root: Path) -> t.Optional[Path]:
        # TODO Check environment variable
        path = Path(project_root, cls.PROFILE_FILE)
        if path.exists():
            return path

        path = Path(Path.home(), ".dbt", cls.PROFILE_FILE)
        if path.exists():
            return path

        return None

    @classmethod
    def _read_profile(
        cls, path: Path, context: DbtContext
    ) -> t.Tuple[t.Dict[str, TargetConfig], str]:
        with open(path, "r", encoding="utf-8") as file:
            source = file.read()
        contents = context.load_yaml(context.render(source))

        project_data = contents.get(context.project_name)
        if not project_data:
            raise ConfigError(f"Project '{context.project_name}' does not exist in profiles.")

        outputs = project_data.get("outputs")
        if not outputs:
            raise ConfigError(f"No outputs exist in profiles for Project '{context.project_name}'.")

        targets = {name: TargetConfig.load(output) for name, output in outputs.items()}
        default_target = context.render(project_data.get("target"))
        if default_target not in targets:
            raise ConfigError(
                f"Default target '#{default_target}' not specified in profiles for Project '{context.project_name}'."
            )

        return (targets, default_target)

    def to_sqlmesh(self) -> t.Dict[str, ConnectionConfig]:
        return {name: target.to_sqlmesh() for name, target in self.targets.items()}
