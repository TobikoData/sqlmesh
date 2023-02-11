from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.config.connection import ConnectionConfig
from sqlmesh.dbt.common import project_config_path
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load as yaml_load


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
    def load(
        cls,
        project_root: t.Optional[Path] = None,
        project_name: t.Optional[str] = None,
    ) -> Profile:
        """
        Loads the profile for the specified project

        Args:
            project_root: Path to the root of the DBT project. If not specified,
                          the current directory is used.
            project_name: Name of the DBT project from the project yaml file. If not
                          specified, the name is read from the project yaml file.

        Returns:
            The Profile for the specified project
        """
        project_root = project_root or Path()

        if not project_name:
            project_file = project_config_path(project_root)
            project_name = yaml_load(project_file).get("name")
            if not project_name:
                raise ConfigError(f"{project_file.stem} must include project name.")

        profile_filepath = cls._find_profile(project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found.")

        targets, default_target = cls._read_profile(profile_filepath, project_name)
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
    def _read_profile(cls, path: Path, project: str) -> t.Tuple[t.Dict[str, TargetConfig], str]:
        contents = yaml_load(path)

        project_data = contents.get(project)
        if not project_data:
            raise ConfigError(f"Project '{project}' does not exist in profiles.")

        outputs = project_data.get("outputs")
        if not outputs:
            raise ConfigError(f"No outputs exist in profiles for Project '{project}'.")

        targets = {name: TargetConfig.load(output) for name, output in outputs.items()}
        default_target = project_data.get("target")
        if default_target not in targets:
            raise ConfigError(
                f"Default target '#{default_target}' not specified in profiles for Project '{project}'."
            )

        return (targets, default_target)

    def to_sqlmesh(self) -> t.Dict[str, ConnectionConfig]:
        return {name: target.to_sqlmesh() for name, target in self.targets.items()}
