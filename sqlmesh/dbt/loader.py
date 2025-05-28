from __future__ import annotations

import logging
import sys
import typing as t
import sqlmesh.core.dialect as d
from pathlib import Path
from sqlmesh.core import constants as c
from sqlmesh.core.config import (
    Config,
    ConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.environment import EnvironmentStatements
from sqlmesh.core.loader import CacheBase, LoadedProject, Loader
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.model import Model, ModelCache
from sqlmesh.core.signal import signal
from sqlmesh.dbt.basemodel import BMC, BaseModelConfig
from sqlmesh.dbt.common import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.project import Project
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import (
    JinjaMacroRegistry,
    make_jinja_registry,
)

if sys.version_info >= (3, 12):
    from importlib import metadata
else:
    import importlib_metadata as metadata  # type: ignore

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import Audit, ModelAudit
    from sqlmesh.core.context import GenericContext

logger = logging.getLogger(__name__)


def sqlmesh_config(
    project_root: t.Optional[Path] = None,
    state_connection: t.Optional[ConnectionConfig] = None,
    dbt_target_name: t.Optional[str] = None,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    register_comments: t.Optional[bool] = None,
    **kwargs: t.Any,
) -> Config:
    project_root = project_root or Path()
    context = DbtContext(project_root=project_root)
    profile = Profile.load(context, target_name=dbt_target_name)
    model_defaults = kwargs.pop("model_defaults", ModelDefaultsConfig())
    if model_defaults.dialect is None:
        model_defaults.dialect = profile.target.dialect

    target_to_sqlmesh_args = {}
    if register_comments is not None:
        target_to_sqlmesh_args["register_comments"] = register_comments

    loader = kwargs.pop("loader", DbtLoader)
    if not issubclass(loader, DbtLoader):
        raise ConfigError("The loader must be a DbtLoader.")

    return Config(
        loader=loader,
        model_defaults=model_defaults,
        variables=variables or {},
        **{
            "default_gateway": profile.target_name if "gateways" not in kwargs else "",
            "gateways": {
                profile.target_name: GatewayConfig(
                    connection=profile.target.to_sqlmesh(**target_to_sqlmesh_args),
                    state_connection=state_connection,
                )
            },  # type: ignore
            **kwargs,
        },
    )


class DbtLoader(Loader):
    def __init__(self, context: GenericContext, path: Path) -> None:
        self._projects: t.List[Project] = []
        self._macros_max_mtime: t.Optional[float] = None
        super().__init__(context, path)

    def load(self) -> LoadedProject:
        self._projects = []
        return super().load()

    def _load_scripts(self) -> t.Tuple[MacroRegistry, JinjaMacroRegistry]:
        macro_files = list(Path(self.config_path, "macros").glob("**/*.sql"))

        for file in macro_files:
            self._track_file(file)

        # This doesn't do anything, the actual content will be loaded from the manifest
        return (
            macro.get_registry(),
            JinjaMacroRegistry(),
        )

    def _load_models(
        self,
        macros: MacroRegistry,
        jinja_macros: JinjaMacroRegistry,
        gateway: t.Optional[str],
        audits: UniqueKeyDict[str, ModelAudit],
        signals: UniqueKeyDict[str, signal],
    ) -> UniqueKeyDict[str, Model]:
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")

        def _to_sqlmesh(config: BMC, context: DbtContext) -> Model:
            logger.debug("Converting '%s' to sqlmesh format", config.canonical_name(context))
            return config.to_sqlmesh(context, audit_definitions=audits)

        for project in self._load_projects():
            context = project.context.copy()

            macros_max_mtime = self._macros_max_mtime
            yaml_max_mtimes = self._compute_yaml_max_mtime_per_subfolder(
                project.context.project_root
            )
            cache = DbtLoader._Cache(self, project, macros_max_mtime, yaml_max_mtimes)

            logger.debug("Converting models to sqlmesh")
            # Now that config is rendered, create the sqlmesh models
            for package in project.packages.values():
                context.set_and_render_variables(package.variables, package.name)
                package_models: t.Dict[str, BaseModelConfig] = {**package.models, **package.seeds}

                for model in package_models.values():
                    if (
                        not context.sqlmesh_config.feature_flags.dbt.scd_type_2_support
                        and isinstance(model, ModelConfig)
                        and model.model_kind(context).is_scd_type_2
                    ):
                        logger.info(
                            "Skipping loading Snapshot (SCD Type 2) models due to the feature flag disabling this feature"
                        )
                        continue

                    sqlmesh_model = cache.get_or_load_models(
                        model.path, loader=lambda: [_to_sqlmesh(model, context)]
                    )[0]

                    models[sqlmesh_model.fqn] = sqlmesh_model

            models.update(self._load_external_models(audits, cache))

        return models

    def _load_audits(
        self, macros: MacroRegistry, jinja_macros: JinjaMacroRegistry
    ) -> UniqueKeyDict[str, Audit]:
        audits: UniqueKeyDict = UniqueKeyDict("audits")

        for project in self._load_projects():
            context = project.context

            logger.debug("Converting audits to sqlmesh")
            for package in project.packages.values():
                context.set_and_render_variables(package.variables, package.name)
                for test in package.tests.values():
                    logger.debug("Converting '%s' to sqlmesh format", test.name)
                    audits[test.name] = test.to_sqlmesh(context)

        return audits

    def _load_projects(self) -> t.List[Project]:
        if not self._projects:
            target_name = self.context.selected_gateway

            self._projects = []

            project = Project.load(
                DbtContext(
                    project_root=self.config_path,
                    target_name=target_name,
                    sqlmesh_config=self.config,
                ),
                variables=self.config.variables,
            )

            self._projects.append(project)

            if project.context.target.database != (self.context.default_catalog or ""):
                raise ConfigError("Project default catalog does not match context default catalog")
            for path in project.project_files:
                self._track_file(path)

            context = project.context

            macros_mtimes: t.List[float] = []

            for package_name, package in project.packages.items():
                context.add_sources(package.sources)
                context.add_seeds(package.seeds)
                context.add_models(package.models)
                macros_mtimes.extend(
                    [
                        self._path_mtimes[m.path]
                        for m in package.macros.values()
                        if m.path in self._path_mtimes
                    ]
                )

            for package_name, macro_infos in context.manifest.all_macros.items():
                context.add_macros(macro_infos, package=package_name)

            self._macros_max_mtime = max(macros_mtimes) if macros_mtimes else None

        return self._projects

    def _load_requirements(self) -> t.Tuple[t.Dict[str, str], t.Set[str]]:
        requirements, excluded_requirements = super()._load_requirements()

        target_packages = ["dbt-core"]
        for project in self._load_projects():
            target_packages.append(f"dbt-{project.context.target.type}")

        for target_package in target_packages:
            if target_package in requirements or target_package in excluded_requirements:
                continue
            try:
                requirements[target_package] = metadata.version(target_package)
            except metadata.PackageNotFoundError:
                from sqlmesh.core.console import get_console

                get_console().log_warning(f"dbt package {target_package} is not installed.")

        return requirements, excluded_requirements

    def _load_environment_statements(self, macros: MacroRegistry) -> t.List[EnvironmentStatements]:
        """Loads dbt's on_run_start, on_run_end hooks into sqlmesh's before_all, after_all statements respectively."""

        hooks_by_package_name: t.Dict[str, EnvironmentStatements] = {}
        project_names: t.Set[str] = set()
        dialect = self.config.dialect
        for project in self._load_projects():
            context = project.context
            for package_name, package in project.packages.items():
                context.set_and_render_variables(package.variables, package_name)
                on_run_start: t.List[str] = [
                    on_run_hook.sql
                    for on_run_hook in sorted(package.on_run_start.values(), key=lambda h: h.index)
                ]
                on_run_end: t.List[str] = [
                    on_run_hook.sql
                    for on_run_hook in sorted(package.on_run_end.values(), key=lambda h: h.index)
                ]

                if on_run_start or on_run_end:
                    dependencies = Dependencies()
                    for hook in [*package.on_run_start.values(), *package.on_run_end.values()]:
                        dependencies = dependencies.union(hook.dependencies)

                    statements_context = context.context_for_dependencies(dependencies)
                    jinja_registry = make_jinja_registry(
                        statements_context.jinja_macros, package_name, set(dependencies.macros)
                    )
                    jinja_registry.add_globals(statements_context.jinja_globals)

                    hooks_by_package_name[package_name] = EnvironmentStatements(
                        before_all=[
                            d.jinja_statement(stmt).sql(dialect=dialect)
                            for stmt in on_run_start or []
                        ],
                        after_all=[
                            d.jinja_statement(stmt).sql(dialect=dialect)
                            for stmt in on_run_end or []
                        ],
                        python_env={},
                        jinja_macros=jinja_registry,
                    )
                    project_names.add(package_name)

        return [
            statements
            for _, statements in sorted(
                hooks_by_package_name.items(),
                key=lambda item: 0 if item[0] in project_names else 1,
            )
        ]

    def _compute_yaml_max_mtime_per_subfolder(self, root: Path) -> t.Dict[Path, float]:
        if not root.is_dir():
            return {}

        result = {}
        max_mtime: t.Optional[float] = None

        for nested in root.iterdir():
            try:
                if nested.is_dir():
                    result.update(self._compute_yaml_max_mtime_per_subfolder(nested))
                elif nested.suffix.lower() in (".yaml", ".yml"):
                    yaml_mtime = self._path_mtimes.get(nested)
                    if yaml_mtime:
                        max_mtime = (
                            max(max_mtime, yaml_mtime) if max_mtime is not None else yaml_mtime
                        )
            except PermissionError:
                pass

        if max_mtime is not None:
            result[root] = max_mtime

        return result

    class _Cache(CacheBase):
        MAX_ENTRY_NAME_LENGTH = 200

        def __init__(
            self,
            loader: DbtLoader,
            project: Project,
            macros_max_mtime: t.Optional[float],
            yaml_max_mtimes: t.Dict[Path, float],
        ):
            self._loader = loader
            self._project = project
            self._macros_max_mtime = macros_max_mtime
            self._yaml_max_mtimes = yaml_max_mtimes

            target = t.cast(TargetConfig, project.context.target)
            cache_path = loader.config_path / c.CACHE / target.name
            self._model_cache = ModelCache(cache_path)

        def get_or_load_models(
            self, target_path: Path, loader: t.Callable[[], t.List[Model]]
        ) -> t.List[Model]:
            models = self._model_cache.get_or_load(
                self._cache_entry_name(target_path),
                self._cache_entry_id(target_path),
                loader=loader,
            )
            for model in models:
                model._path = target_path

            return models

        def put(self, models: t.List[Model], path: Path) -> bool:
            return self._model_cache.put(
                models,
                self._cache_entry_name(path),
                self._cache_entry_id(path),
            )

        def get(self, path: Path) -> t.List[Model]:
            return self._model_cache.get(
                self._cache_entry_name(path),
                self._cache_entry_id(path),
            )

        def _cache_entry_name(self, target_path: Path) -> str:
            try:
                path_for_name = target_path.absolute().relative_to(
                    self._project.context.project_root.absolute()
                )
            except ValueError:
                path_for_name = target_path
            name = "__".join(path_for_name.parts).replace(path_for_name.suffix, "")
            if len(name) > self.MAX_ENTRY_NAME_LENGTH:
                return name[len(name) - self.MAX_ENTRY_NAME_LENGTH :]
            return name

        def _cache_entry_id(self, target_path: Path) -> str:
            max_mtime = self._max_mtime_for_path(target_path)
            return "__".join(
                [
                    str(int(max_mtime)) if max_mtime is not None else "na",
                    self._loader.config.fingerprint,
                ]
            )

        def _max_mtime_for_path(self, target_path: Path) -> t.Optional[float]:
            project_root = self._project.context.project_root

            try:
                target_path.absolute().relative_to(project_root.absolute())
            except ValueError:
                return None

            mtimes = [
                self._loader._path_mtimes.get(target_path),
                self._loader._path_mtimes.get(self._project.profile.path),
                # FIXME: take into account which macros are actually referenced in the target model.
                self._macros_max_mtime,
            ]

            cursor = target_path
            while cursor != project_root:
                cursor = cursor.parent
                mtimes.append(self._yaml_max_mtimes.get(cursor))

            non_null_mtimes = [t for t in mtimes if t is not None]
            return max(non_null_mtimes) if non_null_mtimes else None
