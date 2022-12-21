from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.datawarehouse import DataWarehouseConfig
from sqlmesh.dbt.models import ModelConfig, Models
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import yaml


class ProjectConfig:
    """Configuration for a DBT project"""

    DEFAULT_PROJECT_FILE = "dbt_project.yml"

    def __init__(
        self, project_root: Path, project_name: str, raw_config: t.Dict[str, t.Any]
    ):
        """
        Args:
            project_root: Path to the root directory of the DBT project
            project_name: The name of the DBT project within the project file yaml
            raw_config: The raw project file yaml
        """
        self.project_root = project_root
        self.project_name = project_name
        self.project_raw_config = raw_config
        self._datawarehouse_config: t.Optional[DataWarehouseConfig] = None
        self._models: t.Optional[t.Dict[str, ModelConfig]] = None

    @classmethod
    def load(cls, project_root: t.Optional[Path] = None) -> ProjectConfig:
        """
        Loads the configuration for the specified DBT project

        Args:
            project_root: Path to the root directory of the DBT project.
                          Defaults to the current directory if not specified.

        Returns:
            ProjectConfig instance for the specified DBT project
        """
        project_root = project_root or Path()
        project_config_path = Path(project_root, cls.DEFAULT_PROJECT_FILE)
        if not project_config_path.exists():
            raise ConfigError(
                f"Could not find {cls.DEFAULT_PROJECT_FILE} for this project"
            )

        with project_config_path.open(encoding="utf-8") as file:
            contents = yaml.load(file.read())

        project_name = contents.get("name")
        if not project_name:
            raise ConfigError(f"{cls.DEFAULT_PROJECT_FILE} must include project name")

        return ProjectConfig(project_root, project_name, contents)

    @property
    def datawarehouse_config(self) -> DataWarehouseConfig:
        """
        Configuration for the project's target data warehouse

        Returns:
            The configuration for the target data warehouse
        """
        if self._datawarehouse_config is None:
            self._datawarehouse_config = Profile.load(
                self.project_root, self.project_name
            ).datawarehouse_config

        return self._datawarehouse_config

    @property
    def models(self) -> t.Dict[str, ModelConfig]:
        """
        Configuration for all models defined within the project

        Returns:
            Dictionary of model names to model config
        """
        if self._models is None:
            self._models = Models.load(
                self.project_root,
                self.project_name,
                self.datawarehouse_config.schema_,
                self.project_raw_config,
            )

        return self._models
