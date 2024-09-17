# ruff: noqa: E402
from __future__ import annotations

import json
import logging
import os
import re
import typing as t
from argparse import Namespace
from collections import defaultdict
from pathlib import Path

from dbt import constants as dbt_constants, flags

# Override the file name to prevent dbt commands from invalidating the cache.
dbt_constants.PARTIAL_PARSE_FILE_NAME = "sqlmesh_partial_parse.msgpack"

from dbt.adapters.factory import register_adapter, reset_adapters
from dbt.config import Profile, Project, RuntimeConfig
from dbt.config.profile import read_profile
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import do_not_track

from sqlmesh.core import constants as c
from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.builtin import BUILTIN_FILTERS, BUILTIN_GLOBALS, OVERRIDDEN_MACROS
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.package import MacroConfig
from sqlmesh.dbt.seed import SeedConfig
from sqlmesh.dbt.source import SourceConfig
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import (
    MacroInfo,
    MacroReference,
    extract_call_names,
    jinja_call_arg_name,
)

if t.TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Macro, Manifest
    from dbt.contracts.graph.nodes import ManifestNode, SourceDefinition
    from sqlmesh.utils.jinja import CallNames

logger = logging.getLogger(__name__)

TestConfigs = t.Dict[str, TestConfig]
ModelConfigs = t.Dict[str, ModelConfig]
SeedConfigs = t.Dict[str, SeedConfig]
SourceConfigs = t.Dict[str, SourceConfig]
MacroConfigs = t.Dict[str, MacroConfig]


IGNORED_PACKAGES = {"elementary"}
BUILTIN_CALLS = {*BUILTIN_GLOBALS, *BUILTIN_FILTERS}


class ManifestHelper:
    def __init__(
        self,
        project_path: Path,
        profiles_path: Path,
        profile_name: str,
        target: TargetConfig,
        variable_overrides: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.project_path = project_path
        self.profiles_path = profiles_path
        self.profile_name = profile_name
        self.target = target
        self.variable_overrides = variable_overrides or {}

        self.__manifest: t.Optional[Manifest] = None
        self._project_name: str = ""

        self._is_loaded: bool = False
        self._tests_per_package: t.Dict[str, TestConfigs] = defaultdict(dict)
        self._models_per_package: t.Dict[str, ModelConfigs] = defaultdict(dict)
        self._seeds_per_package: t.Dict[str, SeedConfigs] = defaultdict(dict)
        self._sources_per_package: t.Dict[str, SourceConfigs] = defaultdict(dict)
        self._macros_per_package: t.Dict[str, MacroConfigs] = defaultdict(dict)

        self._macro_flatten_dependencies: t.Dict[str, t.Dict[str, Dependencies]] = defaultdict(dict)

        self._tests_by_owner: t.Dict[str, t.List[TestConfig]] = defaultdict(list)
        self._disabled_refs: t.Optional[t.Set[str]] = None
        self._disabled_sources: t.Optional[t.Set[str]] = None
        self._call_cache: FileCache[t.Dict[str, t.List[CallNames]]] = FileCache(
            self.project_path / c.CACHE, "jinja_calls"
        )

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

        self._calls = {k: (v, False) for k, v in (self._call_cache.get("") or {}).items()}

        self._load_macros()
        self._load_sources()
        self._load_tests()
        self._load_models_and_seeds()
        self._is_loaded = True

        self._call_cache.put("", value={k: v for k, (v, used) in self._calls.items() if used})

    def _load_sources(self) -> None:
        for source in self._manifest.sources.values():
            source_config = SourceConfig(
                **_config(source),
                **source.to_dict(),
            )
            self._sources_per_package[source.package_name][source_config.config_name] = (
                source_config
            )

    def _load_macros(self) -> None:
        for macro in self._manifest.macros.values():
            if macro.name.startswith("test_"):
                macro.macro_sql = _convert_jinja_test_to_macro(macro.macro_sql)

            dependencies = Dependencies(macros=_macro_references(self._manifest, macro))
            if not macro.name.startswith("materialization_") and not macro.name.startswith("test_"):
                dependencies = dependencies.union(
                    self._extra_dependencies(macro.macro_sql, macro.package_name)
                )

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
            dependencies = dependencies.union(self._extra_dependencies(sql, node.package_name))
            dependencies = dependencies.union(
                self._flatten_dependencies_from_macros(dependencies.macros, node.package_name)
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
            if (
                node.resource_type not in ("model", "seed", "snapshot")
                or node.package_name in IGNORED_PACKAGES
            ):
                continue

            macro_references = _macro_references(self._manifest, node)
            tests = (
                self._tests_by_owner[node.name]
                + self._tests_by_owner[f"{node.package_name}.{node.name}"]
            )
            node_config = _node_base_config(node)

            node_name = node.name
            node_version = getattr(node, "version", None)
            if node_version:
                node_name = f"{node_name}_v{node_version}"

            if node.resource_type in {"model", "snapshot"}:
                sql = node.raw_code if DBT_VERSION >= (1, 3) else node.raw_sql  # type: ignore
                dependencies = Dependencies(
                    macros=macro_references, refs=_refs(node), sources=_sources(node)
                )
                dependencies = dependencies.union(self._extra_dependencies(sql, node.package_name))
                dependencies = dependencies.union(
                    self._flatten_dependencies_from_macros(dependencies.macros, node.package_name)
                )

                self._models_per_package[node.package_name][node_name] = ModelConfig(
                    sql=sql,
                    dependencies=dependencies,
                    tests=tests,
                    **node_config,
                )
            else:
                self._seeds_per_package[node.package_name][node_name] = SeedConfig(
                    dependencies=Dependencies(macros=macro_references),
                    tests=tests,
                    **node_config,
                )

    @property
    def _manifest(self) -> Manifest:
        if not self.__manifest:
            self.__manifest = self._load_manifest()
        return self.__manifest

    def _load_manifest(self) -> Manifest:
        do_not_track()

        variables = (
            self.variable_overrides
            if DBT_VERSION >= (1, 5)
            else json.dumps(self.variable_overrides)
        )

        args: Namespace = Namespace(
            vars=variables,
            profile=self.profile_name,
            profiles_dir=str(self.profiles_path),
            target=self.target.name,
            macro_debugging=False,
            REQUIRE_RESOURCE_NAMES_WITHOUT_SPACES=True,
        )
        flags.set_from_args(args, None)

        if DBT_VERSION >= (1, 8):
            from dbt_common.context import set_invocation_context  # type: ignore

            set_invocation_context(os.environ)

        profile = self._load_profile()
        project = self._load_project(profile)

        if not any(k in project.models for k in ("start", "+start")):
            raise ConfigError(
                "SQLMesh's requires a start date in order to have a finite range of backfilling data. Add start to the 'models:' block in dbt_project.yml. https://sqlmesh.readthedocs.io/en/stable/integrations/dbt/#setting-model-backfill-start-dates"
            )

        runtime_config = RuntimeConfig.from_parts(project, profile, args)

        self._project_name = project.project_name

        if DBT_VERSION >= (1, 8):
            from dbt.mp_context import get_mp_context  # type: ignore

            register_adapter(runtime_config, get_mp_context())  # type: ignore
        else:
            register_adapter(runtime_config)  # type: ignore

        manifest = ManifestLoader.get_full_manifest(runtime_config)
        reset_adapters()
        return manifest

    def _load_project(self, profile: Profile) -> Project:
        project_renderer = DbtProjectYamlRenderer(profile, cli_vars=self.variable_overrides)
        return Project.from_project_root(str(self.project_path), project_renderer)

    def _load_profile(self) -> Profile:
        profile_renderer = ProfileRenderer(cli_vars=self.variable_overrides)
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

    def _flatten_dependencies_from_macros(
        self,
        macros: t.List[MacroReference],
        default_package: str,
        visited: t.Optional[t.Set[t.Tuple[str, str]]] = None,
    ) -> Dependencies:
        if visited is None:
            visited = set()

        dependencies = Dependencies()
        for macro in macros:
            macro_package = macro.package or default_package

            if (macro_package, macro.name) in visited:
                continue
            visited.add((macro_package, macro.name))

            macro_dependencies = self._macro_flatten_dependencies.get(macro_package, {}).get(
                macro.name
            )
            if not macro_dependencies:
                macro_config = self._macros_per_package[macro_package].get(macro.name)
                if not macro_config:
                    continue

                macro_dependencies = macro_config.dependencies.union(
                    self._flatten_dependencies_from_macros(
                        macro_config.dependencies.macros, macro_package, visited=visited
                    )
                )
                # We don't need flatten macro dependencies. The jinja macro registry takes care of recursive
                # dependencies for us.
                macro_dependencies.macros = []
                self._macro_flatten_dependencies[macro_package][macro.name] = macro_dependencies
            dependencies = dependencies.union(macro_dependencies)
        return dependencies

    def _extra_dependencies(self, target: str, package: str) -> Dependencies:
        # We sometimes observe that the manifest doesn't capture all macros, refs, and sources within a macro.
        # This behavior has been observed with macros like dbt.current_timestamp(), dbt_utils.slugify(), and source().
        # Here we apply our custom extractor to make a best effort to supplement references captured in the manifest.
        dependencies = Dependencies()
        for call_name, node in extract_call_names(target, cache=self._calls):
            if call_name[0] == "config":
                continue
            elif call_name[0] == "source":
                args = [jinja_call_arg_name(arg) for arg in node.args]
                if args and all(arg for arg in args):
                    source = ".".join(args)
                    if not self._is_disabled_source(source):
                        dependencies.sources.add(source)
                dependencies.macros.append(MacroReference(name="source"))
            elif call_name[0] == "ref":
                args = [jinja_call_arg_name(arg) for arg in node.args]
                if args and all(arg for arg in args):
                    ref = ".".join(args)
                    if not self._is_disabled_ref(ref):
                        dependencies.refs.add(ref)
                dependencies.macros.append(MacroReference(name="ref"))
            elif call_name[0] == "var":
                args = [jinja_call_arg_name(arg) for arg in node.args]
                if args and args[0]:
                    dependencies.variables.add(args[0])
                dependencies.macros.append(MacroReference(name="var"))
            elif len(call_name) == 1:
                macro_name = call_name[0]
                if macro_name in BUILTIN_CALLS:
                    continue
                if (
                    f"macro.{package}.{macro_name}" not in self._manifest.macros
                    and f"macro.dbt.{macro_name}" in self._manifest.macros
                ):
                    package_name: t.Optional[str] = "dbt"
                else:
                    # dbt doesn't include the package name for project macros
                    package_name = package if package != self._project_name else None
                _macro_reference_if_not_overridden(
                    package_name, macro_name, dependencies.macros.append
                )
            else:
                if call_name[0] != "adapter":
                    _macro_reference_if_not_overridden(
                        call_name[0], call_name[1], dependencies.macros.append
                    )

        return dependencies


def _macro_reference_if_not_overridden(
    package: t.Optional[str], name: str, if_not_overridden: t.Callable[[MacroReference], None]
) -> None:
    reference = MacroReference(package=package, name=name)
    if reference not in OVERRIDDEN_MACROS:
        if_not_overridden(reference)


def _config(node: t.Union[ManifestNode, SourceDefinition]) -> t.Dict[str, t.Any]:
    return node.config.to_dict()


def _macro_references(
    manifest: Manifest, node: t.Union[ManifestNode, Macro]
) -> t.Set[MacroReference]:
    result: t.Set[MacroReference] = set()
    for macro_node_id in node.depends_on.macros:
        if not macro_node_id:
            continue

        macro_node = manifest.macros[macro_node_id]
        macro_name = macro_node.name
        macro_package = (
            macro_node.package_name if macro_node.package_name != node.package_name else None
        )
        _macro_reference_if_not_overridden(macro_package, macro_name, result.add)
    return result


def _refs(node: ManifestNode) -> t.Set[str]:
    if DBT_VERSION >= (1, 5):
        result = set()
        for r in node.refs:
            ref_name = f"{r.package}.{r.name}" if r.package else r.name
            if getattr(r, "version", None):
                ref_name = f"{ref_name}_v{r.version}"
            result.add(ref_name)
        return result
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
    return {
        **_config(node),
        **node.to_dict(),
        "path": Path(node.original_file_path),
    }


def _convert_jinja_test_to_macro(test_jinja: str) -> str:
    TEST_TAG_REGEX = r"\s*{%\s*test\s+"
    ENDTEST_REGEX = r"{%\s*endtest\s*%}"
    match = re.match(TEST_TAG_REGEX, test_jinja)
    if not match:
        # already a macro
        return test_jinja

    macro = "{% macro test_" + test_jinja[match.span()[-1] :]
    return re.sub(ENDTEST_REGEX, "{% endmacro %}", macro)
