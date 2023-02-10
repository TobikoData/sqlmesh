from __future__ import annotations

import re
import typing as t
from pathlib import Path

from sqlmesh.dbt.common import BaseConfig, project_config_path
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import capture_jinja
from sqlmesh.utils.pydantic import PydanticModel
from sqlmesh.utils.yaml import load as yaml_load

if t.TYPE_CHECKING:
    C = t.TypeVar("C", bound=BaseConfig)

Scope = t.Union[t.Tuple[()], t.Tuple[str, ...]]
ScopedSources = t.Dict[Scope, SourceConfig]
ScopedSeeds = t.Dict[Scope, SeedConfig]
ScopedModels = t.Dict[Scope, ModelConfig]


class ProjectConfig(PydanticModel):
    source_config: ScopedSources
    seed_config: ScopedSeeds
    model_config: ScopedModels


class Package(PydanticModel):
    sources: t.Dict[str, SourceConfig]
    seeds: t.Dict[str, SeedConfig]
    models: t.Dict[str, ModelConfig]
    files: t.List[Path]


class Project:
    """Configuration for a DBT project"""

    def __init__(
        self,
        project_root: Path,
        project_name: str,
        profile: Profile,
        models: t.Dict[str, ModelConfig],
        sources: t.Dict[str, SourceConfig],
        seeds: t.Dict[str, SeedConfig],
        variables: t.Dict[str, t.Any],
        config_paths: t.List[Path],
    ):
        """
        Args:
            project_root: Path to the root directory of the DBT project
            project_name: The name of the DBT project within the project file yaml
            profile: The profile associated with the project
            models: Dict of model name to model config for all models within this project
            sources: Dict of source name to source config for all sources within this project
            seeds: Dict of seed name to seed config for all seeds within this project
            variables: Dict of variable name to variable value for all variables within this project
            config_paths: Paths to all the config files used by the project
        """
        self.project_root = project_root
        self.project_name = project_name
        self.profile = profile
        self.models = models
        self.sources = sources
        self.seeds = seeds
        self.variables = variables
        self.config_paths = config_paths

    @classmethod
    def load(
        cls, project_root: t.Optional[Path] = None, target_name: t.Optional[str] = None
    ) -> Project:
        """
        Loads the configuration for the specified DBT project

        Args:
            project_root: Path to the root directory of the DBT project.
                          Defaults to the current directory if not specified.
            target_name: Name of the profile to use. Defaults to the project's default target name.

        Returns:
            Project instance for the specified DBT project
        """
        project_root = project_root or Path()
        project_file_path = project_config_path(project_root)
        project_yaml = yaml_load(project_file_path)

        project_name = project_yaml.get("name")
        if not project_name:
            raise ConfigError(f"{project_file_path.stem} must include project name.")

        profile = Profile.load(project_root, project_name)
        target = (
            profile.targets[target_name] if target_name else profile.targets[profile.default_target]
        )

        config_paths = [project_file_path]
        project_config = cls._load_project_config(project_yaml, target.schema_)

        models_dirs = [
            Path(project_root, dir) for dir in project_yaml.get("model-paths") or ["models"]
        ]
        models, sources, paths = cls._load_models(models_dirs, project_config)
        config_paths.extend(paths)
        variables = project_yaml.get("vars", {})

        seed_dirs = [Path(project_root, dir) for dir in project_yaml.get("seed-paths") or ["seeds"]]
        seeds, paths = cls._load_seeds(seed_dirs, project_config)
        config_paths.extend(paths)

        return Project(project_root, project_name, profile, models, sources, seeds, variables, config_paths)


    @classmethod
    def _load_models(
        cls,
        models_dirs: t.List[Path],
        project_config: ProjectConfig,
    ) -> t.Tuple[t.Dict[str, ModelConfig], t.Dict[str, SourceConfig], t.List[Path],]:
        """
        Loads the configuration of all models within the DBT project.

        Args:
            models_dirs: List of dirs containing models
            project_config: Configuration from the dbt_project file

        Returns:
            Tuple containing Dicts of model configs, source configs, and list of config files
            used by them
        """
        models: t.Dict[str, ModelConfig] = {}
        sources: t.Dict[str, SourceConfig] = {}
        paths: t.List[Path] = []

        for root in models_dirs:
            # Layer on configs in properties files
            for path in root.glob("**/*.yml"):
                paths.append(path)

                scope = cls._scope_from_path(path, root)
                properties_yaml = yaml_load(path)

                scoped_models = cls._load_config_section_from_properties(
                    properties_yaml, "models", scope, project_config.model_config
                )

                source_configs_in_file = cls._load_sources_config_from_properties(
                    properties_yaml, scope, project_config.source_config
                )
                sources.update(source_configs_in_file)

            # Layer on configs from the model file and create model configs
            for path in root.glob("**/*.sql"):
                paths.append(path)

                scope = cls._scope_from_path(path, root)
                model_config = cls._load_model_config_from_model(path, scope, scoped_models)
                if model_config.table_name:
                    models[model_config.table_name] = model_config

        return (models, sources, paths)

    @classmethod
    def _load_seeds(
        cls,
        seeds_dirs: t.List[Path],
        project_config: ProjectConfig,
    ) -> t.Tuple[t.Dict[str, SeedConfig], t.List[Path],]:
        """
        Loads the configuration of all seeds within the DBT project.

        Args:
            seeds_dirs: List of dirs containing seeds
            project_config: Configuration from the dbt_project file

        Returns:
            Tuple containing Dicts of seed configs and list of config files used by them
        """
        seeds: t.Dict[str, SeedConfig] = {}
        paths: t.List[Path] = []

        for root in seeds_dirs:
            # Layer on configs in properties files
            for path in root.glob("**/*.yml"):
                paths.append(path)

                scope = cls._scope_from_path(path, root)
                properties_yaml = yaml_load(path)

                scoped_seeds = cls._load_config_section_from_properties(
                    properties_yaml, "seeds", scope, project_config.seed_config
                )

            # Layer on configs from the model file and create seed configs
            for path in root.glob("**/*.csv"):
                paths.append(path)

                scope = cls._scope_from_path(path, root)
                seed_config = cls._config_for_scope(scope, scoped_seeds).copy()
                seed_config.path = path
                seeds[path.stem] = seed_config

        return (seeds, paths)

    @classmethod
    def _load_project_config(
        cls,
        project_yaml: t.Dict[str, t.Any],
        target_schema: str,
    ) -> ProjectConfig:
        def load_config(
            data: t.Dict[str, t.Any],
            parent: C,
            scope: Scope,
            scoped_configs: t.Optional[t.Dict[Scope, C]] = None,
        ) -> t.Dict[Scope, C]:
            """Recursively load nested configs in the parsed yaml"""
            scoped_configs = scoped_configs or {}

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
                load_config(value, config, nested_scope, scoped_configs)

            return scoped_configs

        scope = ()
        return ProjectConfig(
            source_config=load_config(project_yaml.get("sources", {}), SourceConfig(), scope),
            seed_config=load_config(
                project_yaml.get("seeds", {}),
                SeedConfig(target_schema=target_schema),
                scope,
            ),
            model_config=load_config(
                project_yaml.get("models", {}),
                ModelConfig(target_schema=target_schema),
                scope,
            ),
        )

    @classmethod
    def _load_config_section_from_properties(
        cls,
        yaml: t.Dict[str, t.Any],
        section_name: str,
        scope: Scope,
        scoped_configs: t.Dict[Scope, C],
    ) -> t.Dict[Scope, C]:
        section_yaml = yaml.get(section_name)
        if not section_yaml:
            return scoped_configs

        for value in section_yaml:
            fields = value.get("config")
            if not fields:
                continue

            model_scope = (*scope, value["name"])
            scoped_configs[model_scope] = cls._config_for_scope(scope, scoped_configs).update_with(
                fields
            )

        return scoped_configs

    @classmethod
    def _load_sources_config_from_properties(
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
            source_name = source["name"]
            source_config = cls._config_for_scope((*scope, source_name), scoped_configs)

            source_config.schema_ = source_name
            config_fields = source.get("config")
            if config_fields:
                source_config = source_config.update_with(config_fields)
                print(source_config.schema_)

            for table in source["tables"]:
                table_name = table["name"]
                table_config = source_config.copy()

                table_config.identifier = table_name
                config_fields = source.get("config")
                if config_fields:
                    table_config = table_config.update_with(config_fields)

                table_config.config_name = f"{source_name}.{table_name}"
                configs[table_name] = table_config

        return configs

    @classmethod
    def _load_model_config_from_model(
        cls, filepath: Path, scope: Scope, configs: t.Dict[Scope, ModelConfig]
    ) -> ModelConfig:
        with filepath.open(encoding="utf-8") as file:
            sql = file.read()

        model_config = cls._config_for_scope(scope, configs).copy(
            update={"path": filepath, "table_name": filepath.stem}
        )

        depends_on = set()
        calls = set()
        sources = set()

        for method, args, kwargs in capture_jinja(sql).calls:
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
            elif method == "source":
                source = ".".join(args + tuple(kwargs.values()))
                if source:
                    sources.add(source)
            else:
                calls.add(method)

        model_config.sql = cls._remove_config_jinja(sql)
        model_config._depends_on = depends_on
        model_config._calls = calls
        model_config._sources = sources

        return model_config

    @classmethod
    def _scope_from_path(cls, path: Path, root_path: Path) -> Scope:
        """
        DBT rolls-up configuration based o the directory structure. Scope mimics this structure,
        building a tuple containing the project name and directories from project root to the file,
        omitting the "models" directory and filename if a properties file.
        """
        path_from_root = path.relative_to(root_path)
        scope = tuple(path_from_root.parts[:-1])
        if path.match("*.sql") or path.match("*.csv"):
            scope = (*scope, path_from_root.stem)
        return scope

    @classmethod
    def _config_for_scope(cls, scope: Scope, configs: t.Dict[Scope, C]) -> C:
        return configs.get(scope) or cls._config_for_scope(scope[0:-1], configs)

    @classmethod
    def _remove_config_jinja(cls, query: str) -> str:
        return re.sub(r"{{\s*config(.|\s)*?}}", "", query).strip()

    @property
    def project_files(self) -> t.List[Path]:
        paths = self.config_paths.copy()
        paths.append(self.profile.path)
        paths.extend(model.path for model in self.models.values())
        paths.extend(seed.path for seed in self.seeds.values())

        return paths
