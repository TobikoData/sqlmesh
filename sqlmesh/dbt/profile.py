from __future__ import annotations

import typing as t
from pathlib import Path

import sqlmesh.dbt.project as p
from sqlmesh.core.config import Config
from sqlmesh.dbt.datawarehouse import DataWarehouseConfig
from sqlmesh.dbt.loader import DbtLoader
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
        connections: t.Dict[str, DataWarehouseConfig],
        default_target: str,
    ):
        """
        Args:
            path: Path to the profile file
            dataware_config
            datwarehouse_config: The data warehouse configuration for the project's target
        """
        self.path = path
        self.connections = connections
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
            The profile connection configurations for the specified project and the profile's default target
        """
        project_root = project_root or Path()

        if not project_name:
            project_file = p.ProjectConfig.project_file(project_root)
            project_name = yaml.load(project_file).get("name")
            if not project_name:
                raise ConfigError(f"{project_file.stem} must include project name")

        profile_filepath = cls._find_profile(project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found")

        connections, default_target = cls._read_profile(profile_filepath, project_name)
        return Profile(profile_filepath, connections, default_target)

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
        cls, path: Path, project: str
    ) -> t.Tuple[t.Dict[str, DataWarehouseConfig], str]:
        contents = yaml_load(path)

        project_data = contents.get(project)
        if not project_data:
            raise ConfigError(f"Project '{project}' does not exist in profiles")

        outputs = project_data.get("outputs")
        if not outputs:
            raise ConfigError(f"No outputs exist in profiles for Project '{project}'")

        connections = {
            name: DataWarehouseConfig.load(output) for name, output in outputs.items()
        }
        default_target = project_data.get("target")
        if default_target not in connections:
            raise ConfigError(
                f"Default target '#{default_target}' not specified in profiles for Project '{project}'"
            )

        return (connections, default_target)

    def to_sqlmesh(self) -> Config:
        # Default target must be first
        targets = list(self.connections)
        targets.remove(self.default_target)
        targets.insert(0, self.default_target)

        return Config(
            connections={
                target: self.connections[target].to_sqlmesh() for target in targets
            },
            loader=DbtLoader(),
        )
