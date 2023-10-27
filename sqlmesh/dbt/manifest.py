from __future__ import annotations

import logging
import re
import typing as t
from argparse import Namespace
from collections import defaultdict
from pathlib import Path

from dbt import flags
from dbt.adapters.factory import register_adapter, reset_adapters
from dbt.config import Profile, Project, RuntimeConfig
from dbt.config.profile import read_profile
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import do_not_track

from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.package import MacroConfig
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import MacroInfo, MacroReference, extract_call_names, nodes

if t.TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Macro, Manifest
    from dbt.contracts.graph.nodes import ManifestNode, SourceDefinition

logger = logging.getLogger(__name__)

TestConfigs = t.Dict[str, TestConfig]
ModelConfigs = t.Dict[str, ModelConfig]
SeedConfigs = t.Dict[str, SeedConfig]
SourceConfigs = t.Dict[str, SourceConfig]
MacroConfigs = t.Dict[str, MacroConfig]


class ManifestHelper:
    def __init__(
        self,
        project_path: Path,
        profiles_path: Path,
        profile_name: str,
        target: TargetConfig,
    ):
        self.project_path = project_path
        self.profiles_path = profiles_path
        self.profile_name = profile_name
        self.target = target

        self.__manifest: t.Optional[Manifest] = None
        self._project_name: str = ""

        self._is_loaded: bool = False
        self._tests_per_package: t.Dict[str, TestConfigs] = defaultdict(dict)
        self._models_per_package: t.Dict[str, ModelConfigs] = defaultdict(dict)
        self._seeds_per_package: t.Dict[str, SeedConfigs] = defaultdict(dict)
        self._sources_per_package: t.Dict[str, SourceConfigs] = defaultdict(dict)
        self._macros_per_package: t.Dict[str, MacroConfigs] = defaultdict(dict)

        self._tests_by_owner: t.Dict[str, t.List[TestConfig]] = defaultdict(list)
        self._disabled_refs: t.Optional[t.Set[str]] = None
        self._disabled_sources: t.Optional[t.Set[str]] = None

    def tests(self, package_name: t.Optional[str] = None) -> TestConfigs:
        self._load_all()
        return self._tests_per_package[package_name or self._project_name]

    def models(self, package_name: t.Optional[str] = None) -> ModelConfigs:
        self._load_all()
        return self._models_per_package[package_name or self._project_name]

    def seeds(self, package_name: t.Optional[str] = None) -> SeedConfigs:
        self._load_all()
        return self._seeds_per_package[package_name or self._project_name]

    def sources(self, package_name: t.Optional[str] = None) -> SourceConfigs:
        self._load_all()
        return self._sources_per_package[package_name or self._project_name]

    def macros(self, package_name: t.Optional[str] = None) -> MacroConfigs:
        self._load_all()
        return self._macros_per_package[package_name or self._project_name]

    @property
    def all_macros(self) -> t.Dict[str, t.Dict[str, MacroInfo]]:
        self._load_all()
        result: t.Dict[str, t.Dict[str, MacroInfo]] = defaultdict(dict)
        for package_name, macro_configs in self._macros_per_package.items():
            for macro_name, macro_config in macro_configs.items():
                result[package_name][macro_name] = macro_config.info
        return result

    def _load_all(self) -> None:
        if self._is_loaded:
            return
        self._load_macros()
        self._load_sources()
        self._load_tests()
        self._load_models_and_seeds()
        self._is_loaded = True

    def _load_sources(self) -> None:
        for source in self._manifest.sources.values():
            source_dict = source.to_dict()
            if source_dict.get("database") == self.target.database:
                source_dict.pop("database", None)  # Only needed if overrides project's db

            source_config = SourceConfig(
                **_config(source),
                **source_dict,
            )
            self._sources_per_package[source.package_name][
                source_config.config_name
            ] = source_config

    def _load_macros(self) -> None:
        for macro in self._manifest.macros.values():
            if macro.name.startswith("test_"):
                macro.macro_sql = _convert_jinja_test_to_macro(macro.macro_sql)

            dependencies = Dependencies(macros=_macro_references(self._manifest, macro))
            if not macro.name.startswith("materialization_") and not macro.name.startswith("test_"):
                dependencies = dependencies.union(self._extra_dependencies(macro.macro_sql))

            self._macros_per_package[macro.package_name][macro.name] = MacroConfig(
                info=MacroInfo(
                    definition=macro.macro_sql,
                    depends_on=dependencies.macros,
                ),
                dependencies=dependencies,
                path=Path(macro.original_file_path),
            )

    def _load_tests(self) -> None:
        for node in self._manifest.nodes.values():
            if node.resource_type != "test":
                continue

            skip_test = False
            refs = _refs(node)
            for ref in refs:
                if self._is_disabled_ref(ref):
                    logger.info(
                        "Skipping test '%s' which references a disabled model '%s'",
                        node.name,
                        ref,
                    )
                    skip_test = True
                    break

            if skip_test:
                continue

            dependencies = Dependencies(
                macros=_macro_references(self._manifest, node),
                refs=refs,
                sources=_sources(node),
            )
            # Implicit dependencies for model test arg
            dependencies.macros.append(MacroReference(package="dbt", name="get_where_subquery"))
            dependencies.macros.append(MacroReference(package="dbt", name="should_store_failures"))

            sql = node.raw_code if DBT_VERSION >= (1, 3) else node.raw_sql  # type: ignore
            dependencies = dependencies.union(self._extra_dependencies(sql))
            dependencies = dependencies.union(
                self._macro_source_ref_dependencies(dependencies.macros, node.package_name)
            )

            test_model = _test_model(node)

            test = TestConfig(
                sql=sql,
                model_name=test_model,
                test_kwargs=node.test_metadata.kwargs if hasattr(node, "test_metadata") else {},
                dependencies=dependencies,
                **_node_base_config(node),
            )
            self._tests_per_package[node.package_name][node.name.lower()] = test
            if test_model:
                self._tests_by_owner[test_model].append(test)

    def _load_models_and_seeds(self) -> None:
        for node in self._manifest.nodes.values():
            if node.resource_type not in ("model", "seed"):
                continue

            macro_references = _macro_references(self._manifest, node)
            tests = (
                self._tests_by_owner[node.name]
                + self._tests_by_owner[f"{node.package_name}.{node.name}"]
            )

            if node.resource_type == "model":
                sql = node.raw_code if DBT_VERSION >= (1, 3) else node.raw_sql  # type: ignore
                dependencies = Dependencies(
                    macros=macro_references, refs=_refs(node), sources=_sources(node)
                )
                dependencies = dependencies.union(self._extra_dependencies(sql))
                dependencies = dependencies.union(
                    self._macro_source_ref_dependencies(dependencies.macros, node.package_name)
                )

                self._models_per_package[node.package_name][node.name] = ModelConfig(
                    sql=sql,
                    dependencies=dependencies,
                    tests=tests,
                    **_node_base_config(node),
                )
            else:
                self._seeds_per_package[node.package_name][node.name] = SeedConfig(
                    dependencies=Dependencies(macros=macro_references),
                    tests=tests,
                    **_node_base_config(node),
                )

    @property
    def _manifest(self) -> Manifest:
        if not self.__manifest:
            self.__manifest = self._load_manifest()
        return self.__manifest

    def _load_manifest(self) -> Manifest:
        do_not_track()

        args: Namespace = Namespace(
            vars={} if DBT_VERSION >= (1, 5) else "{}",
            profile=self.profile_name,
            profiles_dir=str(self.profiles_path),
            target=self.target.name,
            macro_debugging=False,
        )
        flags.set_from_args(args, None)

        profile = self._load_profile()
        project = self._load_project(profile)

        if not any(k in project.models for k in ("start", "+start")):
            raise ConfigError(
                f"SQLMesh's requires a start date in order to have a finite range of backfilling data. Add start to the 'models:' block in dbt_project.yml. https://sqlmesh.readthedocs.io/en/stable/integrations/dbt/#setting-model-backfill-start-dates"
            )

        runtime_config = RuntimeConfig.from_parts(project, profile, args)

        self._project_name = project.project_name

        register_adapter(runtime_config)
        manifest = ManifestLoader.get_full_manifest(runtime_config)
        reset_adapters()
        return manifest

    def _load_project(self, profile: Profile) -> Project:
        project_renderer = DbtProjectYamlRenderer(profile)
        return Project.from_project_root(str(self.project_path), project_renderer)

    def _load_profile(self) -> Profile:
        profile_renderer = ProfileRenderer({})
        raw_profiles = read_profile(str(self.profiles_path))
        return Profile.from_raw_profiles(
            raw_profiles=raw_profiles,
            profile_name=self.profile_name,
            renderer=profile_renderer,
            target_override=self.target.name,
        )

    def _is_disabled_ref(self, ref: str) -> bool:
        if self._disabled_refs is None:
            self._load_disabled()

        return ref in self._disabled_refs  # type: ignore

    def _is_disabled_source(self, source: str) -> bool:
        if self._disabled_sources is None:
            self._load_disabled()

        return source in self._disabled_sources  # type: ignore

    def _load_disabled(self) -> None:
        self._disabled_refs = set()
        self._disabled_sources = set()
        for nodes in self._manifest.disabled.values():
            for node in nodes:
                if node.resource_type in ("model", "snapshot", "seed"):
                    self._disabled_refs.add(f"{node.package_name}.{node.name}")
                    self._disabled_refs.add(node.name)
                elif node.resource_type == "source":
                    self._disabled_sources.add(f"{node.package_name}.{node.name}")

        for node in self._manifest.nodes.values():
            if node.resource_type in ("model", "snapshot", "seed"):
                self._disabled_refs.discard(node.name)
            elif node.resource_type == "source":
                self._disabled_sources.discard(node.name)

    def _macro_source_ref_dependencies(
        self, macros: t.List[MacroReference], default_package: str
    ) -> Dependencies:
        dependencies = Dependencies()
        for macro in macros:
            macro_config = self._macros_per_package[macro.package or default_package].get(
                macro.name
            )
            if macro_config:
                dependencies = dependencies.union(macro_config.dependencies)
        dependencies.macros = []
        return dependencies

    def _extra_dependencies(self, target: str) -> Dependencies:
        # We sometimes observe that the manifest doesn't capture all macros, refs, and sources within a macro.
        # This behavior has been observed with macros like dbt.current_timestamp(), dbt_utils.slugify(), and source().
        # Here we apply our custom extractor to make a best effort to supplement references captured in the manifest.
        dependencies = Dependencies()
        for call_name, node in extract_call_names(target):
            if len(call_name) == 2 and call_name[0] in ("dbt", "dbt_utils"):
                dependencies.macros.append(MacroReference(package=call_name[0], name=call_name[1]))
            elif call_name[0] == "source":
                args = [_jinja_call_arg_name(arg) for arg in node.args]
                if args and all(arg for arg in args):
                    source = ".".join(args)
                    if not self._is_disabled_source(source):
                        dependencies.sources.append(source)
            elif call_name[0] == "ref":
                args = [_jinja_call_arg_name(arg) for arg in node.args]
                if args and all(arg for arg in args):
                    ref = ".".join(args)
                    if not self._is_disabled_ref(ref):
                        dependencies.refs.append(ref)

        return dependencies


def _config(node: t.Union[ManifestNode, SourceDefinition]) -> t.Dict[str, t.Any]:
    return node.config.to_dict()


def _macro_references(
    manifest: Manifest, node: t.Union[ManifestNode, Macro]
) -> t.Set[MacroReference]:
    result = set()
    for macro_node_id in node.depends_on.macros:
        macro_node = manifest.macros[macro_node_id]
        macro_name = macro_node.name
        macro_package = (
            macro_node.package_name if macro_node.package_name != node.package_name else None
        )
        result.add(MacroReference(package=macro_package, name=macro_name))
    return result


def _refs(node: ManifestNode) -> t.Set[str]:
    if DBT_VERSION >= (1, 5):
        return {f"{r.package}.{r.name}" if r.package else r.name for r in node.refs}  # type: ignore
    else:
        return {".".join(r) for r in node.refs}  # type: ignore


def _sources(node: ManifestNode) -> t.Set[str]:
    return {".".join(s) for s in node.sources}


def _model_node_id(model_name: str, package: str) -> str:
    return f"model.{package}.{model_name}"


def _test_model(node: ManifestNode) -> t.Optional[str]:
    attached_node = getattr(node, "attached_node", None)
    if attached_node:
        pieces = attached_node.split(".")
        return pieces[-1] if pieces[0] in ["model", "seed"] else None

    key_name = getattr(node, "file_key_name", None)
    if key_name:
        pieces = key_name.split(".")
        return pieces[-1] if pieces[0] in ["models", "seeds"] else None

    return None


def _node_base_config(node: ManifestNode) -> t.Dict[str, t.Any]:
    node_dict = node.to_dict()
    node_dict.pop("database", None)  # picked up from the `config` attribute
    return {
        **_config(node),
        **node_dict,
        "path": Path(node.original_file_path),
    }


def _convert_jinja_test_to_macro(test_jinja: str) -> str:
    TEST_TAG_REGEX = "\s*{%\s*test\s+"
    ENDTEST_REGEX = "{%\s*endtest\s*%}"
    match = re.match(TEST_TAG_REGEX, test_jinja)
    if not match:
        # already a macro
        return test_jinja

    macro = "{% macro test_" + test_jinja[match.span()[-1] :]
    return re.sub(ENDTEST_REGEX, "{% endmacro %}", macro)


def _jinja_call_arg_name(node: nodes.Node) -> str:
    if isinstance(node, nodes.Const):
        return node.value
    return ""
