from __future__ import annotations

import typing as t
from pathlib import Path

from ruamel.yaml import YAML
from sqlmesh.utils.errors import ConfigError

from sqlmesh.dbt.database import DatabaseConfig
import sqlmesh.dbt.models as m

class ProjectConfig():
    DEFAULT_PROJECT_FILE = "dbt_project.yml"

    def __init__(self, project_root: Path, project_name: str, config: t.Dict[str, t.Any]):
        self.project_root = project_root
        self.project_name = project_name
        self.project_config = config
        self._database = None
        self._models = None

    @classmethod
    def load(cls, project_root: t.Optional[Path]) -> ProjectConfig:
        project_root = project_root or Path()
        project_config_path = Path(project_root, cls.DEFAULT_PROJECT_FILE)
        if not project_config_path.exists():
            raise ConfigError(
                f"Could not find {cls.DEFAULT_PROJECT_FILE} for this project"
            )

        with project_config_path.open(encoding="utf-8") as file:
            contents = YAML().load(file.read())

        project_name = contents.get("name")
        if not project_name:
            raise ConfigError(f"{cls.DEFAULT_PROJECT_FILE} must include project name")

        return ProjectConfig(project_root, project_name, contents)

    @property
    def database(self) -> DatabaseConfig:
        if not self._database:
    #        self._database = Profile(project_root).database
            self._database = DatabaseConfig(**{"type": "snowflake", "schema": "sushi"})
        
        return self._database

    @property 
    def models(self) -> t.Dict[str, m.ModelConfig]:
        if not self._models:
            self._models = m.Models.load(self.project_root, self.database.schema_, self)
        
        return self._models
