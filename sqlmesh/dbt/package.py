from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.dbt.common import (
    PROJECT_FILENAME,
    BaseConfig,
    DbtContext,
    SqlStr,
    load_yaml,
)
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroExtractor, MacroInfo
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    C = t.TypeVar("C", bound=BaseConfig)

Scope = t.Union[t.Tuple[()], t.Tuple[str, ...]]
ScopedSources = t.Dict[Scope, SourceConfig]
ScopedSeeds = t.Dict[Scope, SeedConfig]
ScopedModels = t.Dict[Scope, ModelConfig]


class ProjectConfig(PydanticModel):
    """Class to contain configuration from dbt_project.yml"""

    source_config: ScopedSources = {(): SourceConfig()}
    seed_config: ScopedSeeds = {(): SeedConfig()}
    model_config: ScopedModels = {(): ModelConfig()}


class Package(PydanticModel):
    """Class to contain package configuration"""

    sources: t.Dict[str, SourceConfig]
    seeds: t.Dict[str, SeedConfig]
    models: t.Dict[str, ModelConfig]
    variables: t.Dict[str, t.Any]
    macros: t.Dict[str, MacroInfo]
    files: t.Set[Path]


class PackageLoader:
    """Loader for DBT packages"""

    def __init__(self, context: DbtContext, overrides: ProjectConfig):
        self._context = context.copy()
        self._overrides = overrides
        self._config_paths: t.Set[Path] = set()
        self.project_config = ProjectConfig()

    def load(self) -> Package:
        """
        Loads the specified package.

        Returns:
            Package containing the configuration found within this package
        """
        project_file_path = Path(self._context.project_root, PROJECT_FILENAME)
        if not project_file_path.exists():
            raise ConfigError(f"Could not find {PROJECT_FILENAME} in {self._context.project_root}")

        self._config_paths.add(project_file_path)
        project_yaml = load_yaml(project_file_path)

        self._package_name = self._context.render(project_yaml.get("name", ""))

        # Only include globally-scoped variables (i.e. filter out the package-scoped ones)
        self._context.variables = {
            var: value
            for var, value in project_yaml.get("vars", {}).items()
            if not isinstance(value, dict)
        }

        self._load_project_config(project_yaml)

        models_dirs = [
            Path(self._context.project_root, self._context.render(dir))
            for dir in project_yaml.get("model-paths") or ["models"]
        ]
        models, sources = self._load_models(models_dirs)

        seeds_dirs = [
            Path(self._context.project_root, self._context.render(dir))
            for dir in project_yaml.get("seed-paths") or ["seeds"]
        ]
        seeds = self._load_seeds(seeds_dirs)

        macros_dirs = [
            Path(self._context.project_root, self._context.render(dir))
            for dir in project_yaml.get("macro-paths") or ["macros"]
        ]
        macros = self._load_macros(macros_dirs)

        return Package(
            models=models,
            sources=sources,
            seeds=seeds,
            variables=self._context.variables,
            macros=macros,
            files=self._config_paths,
        )

    def _load_project_config(self, yaml: t.Dict[str, t.Any]) -> None:
        def load_config(
            data: t.Dict[str, t.Any],
            parent: C,
            scope: Scope,
            scoped_configs: t.Optional[t.Dict[Scope, C]] = None,
        ) -> t.Dict[Scope, C]:
            """Recursively loads config and nested configs in the provided yaml"""
            scoped_configs = scoped_configs or {}

            config_fields = parent.all_fields()

            nested_config = {}
            fields = {}
            for key, value in data.items():
                if key.startswith("+") or key in config_fields or not isinstance(value, dict):
                    fields[key[1:]] = value
                else:
                    nested_config[key] = value

            config = parent.update_with(fields)
            scoped_configs[scope] = config
            for key, value in nested_config.items():
                nested_scope = (*scope, key)
                load_config(value, config, nested_scope, scoped_configs)

            return scoped_configs

        scope = ()
        self.project_config.source_config = load_config(
            yaml.get("sources", {}), SourceConfig(), scope
        )
        self.project_config.seed_config = load_config(
            yaml.get("seeds", {}),
            SeedConfig(target_schema=self._context.target.schema_),
            scope,
        )
        self.project_config.model_config = load_config(
            yaml.get("models", {}),
            ModelConfig(target_schema=self._context.target.schema_),
            scope,
        )

    def _load_models(
        self, models_dirs: t.List[Path]
    ) -> t.Tuple[t.Dict[str, ModelConfig], t.Dict[str, SourceConfig]]:
        """
        Loads the configuration of all models within the DBT project.

        Args:
            models_dirs: List of dirs containing models

        Returns:
            Tuple containing Dicts of model configs, source configs, and list of config files
            used by them
        """
        models: t.Dict[str, ModelConfig] = {}
        sources: t.Dict[str, SourceConfig] = {}

        for root in models_dirs:
            # Layer on configs in properties files
            for path in root.glob("**/*.yml"):
                self._config_paths.add(path)

                scope = self._scope_from_path(path, root)
                properties_yaml = load_yaml(path)

                self._load_config_section_from_properties(
                    properties_yaml, "models", scope, self.project_config.model_config
                )

                source_configs_in_file = self._load_sources_config_from_properties(
                    properties_yaml, scope, self.project_config.source_config
                )
                sources.update(source_configs_in_file)

            # Layer on configs from the model file and create model configs
            for path in root.glob("**/*.sql"):
                self._config_paths.add(path)

                scope = self._scope_from_path(path, root)
                model_config = self._load_model_config_from_model(path, scope)
                model_config.update_with(self._overridden_model_fields(scope))
                if model_config.enabled and model_config.table_name:
                    models[model_config.table_name] = model_config

        return (models, sources)

    def _load_seeds(
        self,
        seeds_dirs: t.List[Path],
    ) -> t.Dict[str, SeedConfig]:
        """
        Loads the configuration of all seeds within the DBT project.

        Args:
            seeds_dirs: List of dirs containing seeds

        Returns:
            Tuple containing Dicts of seed configs and list of config files used by them
        """
        seeds: t.Dict[str, SeedConfig] = {}

        for root in seeds_dirs:
            # Layer on configs in properties files
            for path in root.glob("**/*.yml"):
                self._config_paths.add(path)

                scope = self._scope_from_path(path, root)
                properties_yaml = load_yaml(path)

                self._load_config_section_from_properties(
                    properties_yaml, "seeds", scope, self.project_config.seed_config
                )

            # Layer on configs from the model file and create seed configs
            for path in root.glob("**/*.csv"):
                self._config_paths.add(path)

                scope = self._scope_from_path(path, root)
                seed_config = self._config_for_scope(scope, self.project_config.seed_config).copy()
                seed_config.update_with(self._overridden_seed_fields(scope))
                seed_config.path = path
                if seed_config.enabled:
                    seeds[path.stem] = seed_config

        return seeds

    def _load_macros(self, macros_dirs: t.List[Path]) -> t.Dict[str, MacroInfo]:
        macros: t.Dict[str, MacroInfo] = {}

        for root in macros_dirs:
            for path in root.glob("**/*.sql"):
                macros.update(self._load_macro_file(path))

        return macros

    def _load_macro_file(self, path: Path) -> t.Dict[str, MacroInfo]:
        self._config_paths.add(path)
        with open(path, mode="r", encoding="utf8") as file:
            return MacroExtractor().extract(file.read())

    def _load_config_section_from_properties(
        self,
        yaml: t.Dict[str, t.Any],
        section_name: str,
        scope: Scope,
        scoped_configs: t.Dict[Scope, C],
    ) -> None:
        section_yaml = yaml.get(section_name)
        if not section_yaml:
            return

        for value in section_yaml:
            fields = value.get("config")
            if not fields:
                continue

            model_scope = (*scope, value["name"])
            scoped_configs[model_scope] = self._config_for_scope(scope, scoped_configs).update_with(
                fields
            )

    def _load_sources_config_from_properties(
        self,
        properties_yaml: t.Dict[str, t.Any],
        scope: Scope,
        scoped_configs: t.Dict[Scope, SourceConfig],
    ) -> t.Dict[str, SourceConfig]:
        sources_yaml = properties_yaml.get("sources")
        if not sources_yaml:
            return {}

        configs = {}

        for source in sources_yaml:
            source_name = source["name"]
            source_config = self._config_for_scope((*scope, source_name), scoped_configs)

            source_config.schema_ = source_name
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

                table_config.config_name = f"{source_name}.{table_name}"
                if table_config.enabled:
                    configs[table_config.config_name] = table_config

        return configs

    def _load_model_config_from_model(self, filepath: Path, scope: Scope) -> ModelConfig:
        with filepath.open(encoding="utf-8") as file:
            sql = file.read()

        model_config = self._config_for_scope(scope, self.project_config.model_config).copy(
            update={"path": filepath}
        )
        model_config.sql = SqlStr(sql)

        return model_config

    def _overridden_source_fields(self, scope: Scope) -> t.Dict[str, t.Any]:
        return self._overridden_fields(scope, self._overrides.source_config)

    def _overridden_model_fields(self, scope: Scope) -> t.Dict[str, t.Any]:
        return self._overridden_fields(scope, self._overrides.model_config)

    def _overridden_seed_fields(self, scope: Scope) -> t.Dict[str, t.Any]:
        return self._overridden_fields(scope, self._overrides.seed_config)

    def _overridden_fields(self, scope: Scope, config: t.Dict[Scope, C]) -> t.Dict[str, t.Any]:
        return self._config_for_scope(scope, config).dict(exclude_defaults=True, exclude_none=True)

    def _scope_from_path(self, path: Path, root_path: Path) -> Scope:
        """
        DBT rolls-up configuration based o the directory structure. Scope mimics this structure,
        building a tuple containing the project name and directories from project root to the file,
        omitting the "models" directory and filename if a properties file.
        """
        path_from_root = path.relative_to(root_path)
        scope = (self._package_name, *path_from_root.parts[:-1])
        if path.match("*.sql") or path.match("*.csv"):
            scope = (*scope, path_from_root.stem)
        return scope

    def _config_for_scope(self, scope: Scope, configs: t.Dict[Scope, C]) -> C:
        return configs.get(scope) or self._config_for_scope(scope[0:-1], configs)
