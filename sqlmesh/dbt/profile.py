from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.database import DatabaseConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import yaml


class Profile:
    PROFILE_FILE = "profiles.yml"

    def __init__(self, database: DatabaseConfig):
        self.database = database

    @classmethod
    def load(
        cls, project_root: Path, project_name: str, *, target: t.Optional[str] = None
    ) -> Profile:
        profile_filepath = cls._find_profile(project_root)
        if not profile_filepath:
            raise ConfigError(f"{cls.PROFILE_FILE} not found")

        profile_data = cls._read_profile(profile_filepath, project_name, target)
        return Profile(DatabaseConfig.parse(profile_data))

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
