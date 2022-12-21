from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.datawarehouse import DataWarehouseConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import yaml


class Profile:
    """
    A class to read DBT profiles and obtain the project's target data warehouse configuration
    """

    PROFILE_FILE = "profiles.yml"

    def __init__(self, datawarehouse_config: DataWarehouseConfig):
        """
        Args:
            datwarehouse_config: The data warehouse configuration for the project's target
        """
        self.datawarehouse_config = datawarehouse_config

    @classmethod
    def load(
        cls, project_root: Path, project_name: str, *, target: t.Optional[str] = None
    ) -> Profile:
        """
        Loads the profile for the specified project

        Args:
            project_root: Path to the root of the DBT project
            project_name: Name of the DBT project specified in the project yaml
            target: The project's profile target to load. If not specified,
                    the target defined in the project's profile will be used.

        Returns:
            The profile configuration for the specified project and target
        """
        profile_filepath = cls._find_profile(project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found")

        profile_data = cls._read_profile(profile_filepath, project_name, target)
        return Profile(DataWarehouseConfig.load(profile_data))

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
        cls, path: Path, project: str, target: t.Optional[str]
    ) -> t.Dict[str, t.Any]:
        with path.open(encoding="utf-8") as file:
            contents = yaml.load(file.read())

        project_data = contents.get(project)
        if not project_data:
            raise ConfigError(f"Project '{project}' does not exist in profile")

        if not target:
            target = project_data.get("target")
            if not target:
                raise ConfigError(
                    f"No targets found in profile for Project '{project}'"
                )

        profile_data = project_data.get("outputs")
        if profile_data:
            profile_data = profile_data.get(target)
        if not profile_data:
            raise ConfigError(
                f"Target '{target}' does not exist in profile for Project '{project}'"
            )

        return profile_data
