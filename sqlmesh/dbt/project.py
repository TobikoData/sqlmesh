from __future__ import annotations

import re
import typing as t
from pathlib import Path

from sqlmesh.dbt.common import BaseConfig
from sqlmesh.dbt.datawarehouse import DataWarehouseConfig
from sqlmesh.dbt.models import ModelConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.sources import SourceConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import capture_jinja
from sqlmesh.utils.yaml import yaml

if t.TYPE_CHECKING:
    C = t.TypeVar("C", bound=BaseConfig)
    Scope = t.Union[t.Tuple[()], t.Tuple[str, ...]]


class ProjectConfig:
    """Configuration for a DBT project"""

    DEFAULT_PROJECT_FILE = "dbt_project.yml"

    def __init__(
        self,
        project_root: Path,
        project_name: str,
        datawarehouse: DataWarehouseConfig,
        models: t.Dict[str, ModelConfig],
        sources: t.Dict[str, SourceConfig],
    ):
        """
        Args:
            project_root: Path to the root directory of the DBT project
            project_name: The name of the DBT project within the project file yaml
            datawarehouse: The project's DataWarehouse configuration for the specified target
            models: Dict of model name to model config for all models within this project
            sources: Dict of source name to source config for all sources within this project
        """
        self.project_root = project_root
        self.project_name = project_name
        self.datawarehouse_config = datawarehouse
        self.models = models
        self.sources = sources

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
            project_yaml = yaml.load(file.read())

        project_name = project_yaml.get("name")
        if not project_name:
            raise ConfigError(f"{cls.DEFAULT_PROJECT_FILE} must include project name")

        dw_config = Profile.load(project_root, project_name).datawarehouse_config

        models, sources = cls._load_models_and_sources(
            project_root, project_name, dw_config.schema_, project_yaml
        )

        return ProjectConfig(project_root, project_name, dw_config, models, sources)

    @classmethod
    def _load_models_and_sources(
        cls,
        project_root: Path,
        project_name: str,
        project_schema: str,
        project_yaml: t.Dict[str, t.Any],
    ) -> t.Tuple[t.Dict[str, ModelConfig], t.Dict[str, SourceConfig]]:
        """
        Loads the configuration of all models within the specified DBT project.

        Args:
            project_root: Path to the root directory of the DBT project
            project_name: Name of the DBT project as defined in the project yaml
            project_schema: The target database schema
            project_yaml: The yaml from the project file

        Returns:
            Tuple of Dict of model names to model configuration and Dict of
        """
        model_configs: t.Dict[str, ModelConfig] = {}
        source_configs: t.Dict[str, SourceConfig] = {}

        # Start with configs in the project file
        scoped_models: t.Dict[Scope, ModelConfig] = {
            (): ModelConfig(schema=project_schema)
        }
        scoped_models = cls._load_project_config(project_yaml, "models", scoped_models)

        scoped_sources: t.Dict[Scope, SourceConfig] = {
            (): SourceConfig(schema=project_schema)
        }
        scoped_sources = cls._load_project_config(
            project_yaml, "sources", scoped_sources
        )

        # Layer on configs in property files
        for filepath in project_root.glob("models/**/*.yml"):
            scope = cls._scope_from_path(filepath, project_root, project_name)
            with filepath.open(encoding="utf-8") as file:
                properties_yaml = yaml.load(file.read())
                scoped_models = cls._load_properties_model_config(
                    properties_yaml, scope, scoped_models
                )
                property_source_configs = cls._load_properties_sources_config(
                    properties_yaml, scope, scoped_sources
                )
                source_configs.update(property_source_configs)

        # Layer on configs from the model file and create model configs
        for filepath in project_root.glob("models/**/*.sql"):
            scope = cls._scope_from_path(filepath, project_root, project_name)
            model_config = cls._load_model_config(filepath, scope, scoped_models)
            if model_config.table_name:
                model_configs[model_config.table_name] = model_config

        return (model_configs, source_configs)

    @classmethod
    def _load_project_config(
        cls,
        project_yaml: t.Dict[str, t.Any],
        name: str,
        scoped_configs: t.Dict[Scope, C],
    ) -> t.Dict[Scope, C]:
        yaml = project_yaml.get(name)
        if not yaml:
            return scoped_configs

        def load_config(data, parent, scope):
            nested_config = {}
            fields = {}
            for key, value in data.items():
                if key.startswith("+"):
                    fields[key[1:]] = value
                else:
                    nested_config[key] = value

            config = parent.update_with(fields)
            scoped_configs[scope] = config
            for key, value in nested_config.items():
                nested_scope = (*scope, key)
                load_config(value, config, nested_scope)

        scope = ()
        load_config(yaml, scoped_configs[scope], scope)
        return scoped_configs

    @classmethod
    def _load_properties_model_config(
        cls,
        properties_yaml: t.Dict[str, t.Any],
        scope: Scope,
        scoped_configs: t.Dict[Scope, ModelConfig],
    ) -> t.Dict[Scope, ModelConfig]:
        models_yaml = properties_yaml.get("models")
        if not models_yaml:
            return scoped_configs

        for value in models_yaml:
            fields = value.get("config")
            if not fields:
                continue

            model_scope = (*scope, value["name"])
            scoped_configs[model_scope] = cls._config_for_scope(
                scope, scoped_configs
            ).update_with(fields)

        return scoped_configs

    @classmethod
    def _load_properties_sources_config(
        cls,
        properties_yaml: t.Dict[str, t.Any],
        scope: Scope,
        scoped_configs: t.Dict[Scope, SourceConfig],
    ) -> t.Dict[str, SourceConfig]:
        sources_yaml = properties_yaml.get("sources")
        if not sources_yaml:
            return {}

        configs = {}

        for source in sources_yaml:
            schema = source["name"]
            source_config = cls._config_for_scope((*scope, schema), scoped_configs)

            source_config.schema_ = schema
            config_fields = source.get("config")
            if config_fields:
                source_config = source_config.update_with(config_fields)

            for table in source["tables"]:
                table_name = table["name"]
                table_config = source_config.copy()

                table_config.identifier = table_name
                config_fields = source.get("config")
                if config_fields:
                    table_config = table_config.update_with(config_fields)

                configs[table_name] = table_config

        return configs

    @classmethod
    def _load_model_config(
        cls, filepath: Path, scope: Scope, configs: t.Dict[Scope, ModelConfig]
    ) -> ModelConfig:
        with filepath.open(encoding="utf-8") as file:
            sql = file.read()

        model_config = cls._config_for_scope(scope, configs).copy(
            update={"path": filepath, "table_name": filepath.stem}
        )

        depends_on = set()
        calls = set()

        for method, args, kwargs in capture_jinja(sql).calls:
            calls.add(method)
            if method == "config":
                if args:
                    if isinstance(args[0], dict):
                        model_config.replace(model_config.update_with(args[0]))
                if kwargs:
                    model_config.replace(model_config.update_with(kwargs))
            elif method == "ref":
                dep = ".".join(args + tuple(kwargs.values()))
                if dep:
                    depends_on.add(dep)

        model_config.sql = cls._remove_config_jinja(sql)
        model_config._depends_on = depends_on
        model_config._calls = calls

        return model_config

    @classmethod
    def _scope_from_path(cls, path: Path, root_path: Path, project_name: str) -> Scope:
        """
        DBT rolls-up configuration based on the project name and the directory structure.
        Scope mimics this structure, building a tuple containing the project name and
        directories from project root to the file, omitting the "models" directory and
        filename if a properties file.
        """
        path_from_root = path.relative_to(root_path)
        scope = (project_name, *path_from_root.parts[1:-1])
        if path.match("*.sql"):
            scope = (*scope, path_from_root.stem)
        return scope

    @classmethod
    def _config_for_scope(cls, scope: Scope, configs: t.Dict[Scope, C]) -> C:
        return configs.get(scope) or cls._config_for_scope(scope[0:-1], configs)

    @classmethod
    def _remove_config_jinja(cls, query: str) -> str:
        return re.sub(r"{{\s*config(.|\s)*?}}", "", query).strip()
