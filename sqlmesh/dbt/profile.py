from __future__ import annotations

import typing as t
from pathlib import Path
from ruamel.yaml import YAML

from sqlmesh.dbt.database import DatabaseConfig
from sqlmesh.dbt.project import ProjectConfig
from sqlmesh.utils.errors import ConfigError


class Profile:
    PROFILE_FILENAME = "profiles.yml"

    def __init__(self, project_root: Path, *, target: t.Optional[str] = None):
        project_root = project_root or Path()
        project_name = ProjectConfig.load(project_root).project_name
        profile_path = self._find_profile(project_root)
        if not profile_path:
            raise ConfigError(f"{self.PROFILE_FILENAME} not found")

        profile_data = self._read_profile(profile_path, project_name, target)
        self.database = DatabaseConfig.parse(profile_data)

    def _find_profile(self, project_root: Path) -> t.Optional[Path]:
        # TODO Check environment variable
        path = Path(project_root, self.PROFILE_FILENAME)
        if path.exists():
            return path

        path = Path(Path.home(), self.PROFILE_FILENAME)
        if path.exists():
            return path

        return None

    def _read_profile(
        self, path: Path, project: str, target: t.Optional[str]
    ) -> t.Dict[str, t.Any]:
        with path.open(encoding="utf-8") as file:
            contents = YAML().load(file.read())

        project_data = contents.get(project)
        if not project_data:
            raise ConfigError(
                f"Project '{project}' does not exist in {self.PROFILE_FILENAME}"
            )

        if not target:
            target = project_data.get("target")
            if not target:
                raise ConfigError(
                    f"No target found for Project '{project}' in {self.PROFILE_FILENAME}"
                )

        profile_data = project_data.get(target)
        if not profile_data:
            raise ConfigError(
                f"Profile for Project '{project}' and Target '{target}' not found in {self.PROFILE_FILENAME}"
            )

        return profile_data
