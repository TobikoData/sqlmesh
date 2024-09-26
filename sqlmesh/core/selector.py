from __future__ import annotations

import fnmatch
import logging
import typing as t
from collections import defaultdict
from pathlib import Path

from sqlglot import exp

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.environment import Environment
from sqlmesh.core.loader import update_model_schemas
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.git import GitClient

logger = logging.getLogger(__name__)


if t.TYPE_CHECKING:
    from sqlmesh.core.audit import ModelAudit
    from sqlmesh.core.model import Model
    from sqlmesh.core.state_sync import StateReader


class Selector:
    def __init__(
        self,
        state_reader: StateReader,
        models: UniqueKeyDict[str, Model],
        audits: t.Dict[str, ModelAudit],
        context_path: Path = Path("."),
        dag: t.Optional[DAG[str]] = None,
        default_catalog: t.Optional[str] = None,
        dialect: t.Optional[str] = None,
    ):
        self._state_reader = state_reader
        self._models = models
        self._audits = audits
        self._context_path = context_path
        self._default_catalog = default_catalog
        self._dialect = dialect
        self._git_client = GitClient(context_path)
        self.__models_by_tag: t.Optional[t.Dict[str, t.Set[str]]] = None

        if dag is None:
            self._dag: DAG[str] = DAG()
            for fqn, model in models.items():
                self._dag.add(fqn, model.depends_on)
        else:
            self._dag = dag

    def select_models(
        self,
        model_selections: t.Iterable[str],
        target_env_name: str,
        fallback_env_name: t.Optional[str] = None,
        ensure_finalized_snapshots: bool = False,
    ) -> UniqueKeyDict[str, Model]:
        """Given a set of selections returns models from the current state with names matching the
        selection while sourcing the remaining models from the target environment.

        Args:
            model_selections: A set of selections.
            target_env_name: The name of the target environment.
            fallback_env_name: The name of the fallback environment that will be used if the target
                environment doesn't exist.
            ensure_finalized_snapshots: Whether to source environment snapshots from the latest finalized
                environment state, or to use whatever snapshots are in the current environment state even if
                the environment is not finalized.

        Returns:
            A dictionary of models.
        """
        target_env = self._state_reader.get_environment(Environment.sanitize_name(target_env_name))
        if target_env and target_env.expired:
            target_env = None

        if not target_env and fallback_env_name:
            target_env = self._state_reader.get_environment(
                Environment.sanitize_name(fallback_env_name)
            )

        env_models: t.Dict[str, Model] = {}
        if target_env:
            environment_snapshot_infos = (
                target_env.snapshots
                if not ensure_finalized_snapshots
                else target_env.finalized_or_current_snapshots
            )
            env_models = {
                s.name: s.model
                for s in self._state_reader.get_snapshots(environment_snapshot_infos).values()
                if s.is_model
            }

        all_selected_models = self.expand_model_selections(
            model_selections, models={**self._models, **env_models}
        )

        dag: DAG[str] = DAG()
        subdag = set()

        for fqn in all_selected_models:
            if fqn not in subdag:
                subdag.add(fqn)
                subdag.update(self._dag.downstream(fqn))

        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        all_model_fqns = set(self._models) | set(env_models)
        needs_update = False

        def get_model(fqn: str) -> t.Optional[Model]:
            if fqn not in all_selected_models and fqn in env_models:
                # Unselected modified or added model.
                return env_models[fqn]
            if fqn in all_selected_models and fqn in self._models:
                # Selected modified or removed model.
                return self._models[fqn]
            return None

        for fqn in all_model_fqns:
            model = get_model(fqn)

            if not model:
                continue

            if model.fqn in subdag:
                dag.add(model.fqn, model.depends_on)

                for dep in model.depends_on:
                    schema = model.mapping_schema

                    for part in exp.to_table(dep).parts:
                        schema = schema.get(part.sql()) or {}

                    parent = get_model(dep)

                    parent_schema = {
                        c: t.sql(dialect=model.dialect)
                        for c, t in ((parent and parent.columns_to_types) or {}).items()
                    }

                    if schema != parent_schema:
                        model = model.copy(update={"mapping_schema": {}})
                        needs_update = True
                        break

            models[model.fqn] = model

        if needs_update:
            update_model_schemas(
                dag, models=models, audits=self._audits, context_path=self._context_path
            )

        return models

    def expand_model_selections(
        self, model_selections: t.Iterable[str], models: t.Optional[t.Dict[str, Model]] = None
    ) -> t.Set[str]:
        """Expands a set of model selections into a set of model names.

        Args:
            model_selections: A set of model selections.

        Returns:
            A set of model names.
        """
        results: t.Set[str] = set()
        models = models or self._models
        models_by_tags: t.Optional[t.Dict[str, t.Set[str]]] = None

        for selection in model_selections:
            sub_results: t.Optional[t.Set[str]] = None

            def add_sub_results(sr: t.Set[str]) -> None:
                nonlocal sub_results
                if sub_results is None:
                    sub_results = sr
                else:
                    sub_results &= sr

            sub_selections = [s.strip() for s in selection.split("&")]
            for sub_selection in sub_selections:
                if not sub_selection:
                    continue

                if sub_selection.startswith("tag:"):
                    if models_by_tags is None:
                        models_by_tag = defaultdict(set)
                        for model in models.values():
                            for tag in model.tags:
                                models_by_tag[tag.lower()].add(model.fqn)
                    add_sub_results(
                        self._expand_model_tag(sub_selection[4:], models, models_by_tag)
                    )
                elif sub_selection.startswith(("git:", "+git:")):
                    sub_selection = sub_selection.replace("git:", "")
                    add_sub_results(self._expand_git(sub_selection, models))
                else:
                    add_sub_results(self._expand_model_name(sub_selection, models))

            if sub_results:
                results.update(sub_results)
            else:
                logger.warning(f"Expression '{selection}' doesn't match any models.")

        return results

    def _expand_git(self, target_branch: str, models: t.Dict[str, Model]) -> t.Set[str]:
        results: t.Set[str] = set()

        (
            target_branch,
            include_upstream,
            include_downstream,
        ) = self._get_value_and_dependency_inclusion(target_branch)

        git_modified_files = {
            *self._git_client.list_untracked_files(),
            *self._git_client.list_uncommitted_changed_files(),
            *self._git_client.list_committed_changed_files(target_branch=target_branch),
        }
        matched_models = {m.fqn for m in self._models.values() if m._path in git_modified_files}

        if not matched_models:
            logger.warning(f"Expression 'git:{target_branch}' doesn't match any models.")
            return matched_models

        for model_fqn in matched_models:
            results.update(
                self._get_models(model_fqn, include_upstream, include_downstream, models)
            )

        return results

    def _expand_model_name(self, selection: str, models: t.Dict[str, Model]) -> t.Set[str]:
        results = set()

        (
            selection,
            include_upstream,
            include_downstream,
        ) = self._get_value_and_dependency_inclusion(selection)

        matched_models = set()

        if "*" in selection:
            for model in models.values():
                if fnmatch.fnmatchcase(model.name, selection):
                    matched_models.add(model.fqn)
        else:
            model_fqn = normalize_model_name(selection, self._default_catalog, self._dialect)
            if model_fqn in models:
                matched_models.add(model_fqn)

        if not matched_models:
            logger.warning(f"Expression '{selection}' doesn't match any models.")

        for model_fqn in matched_models:
            results.update(
                self._get_models(model_fqn, include_upstream, include_downstream, models)
            )
        return results

    def _expand_model_tag(
        self, tag_selection: str, models: t.Dict[str, Model], models_by_tag: t.Dict[str, t.Set[str]]
    ) -> t.Set[str]:
        """
        Expands a set of model tags into a set of model names.
        The tag matching is case-insensitive and supports wildcards and + prefix and suffix to
        include upstream and downstream models.

        Args:
            tag_selection: A tag to match models against.

        Returns:
            A set of model names.
        """
        result = set()
        matched_tags = set()
        (
            selection,
            include_upstream,
            include_downstream,
        ) = self._get_value_and_dependency_inclusion(tag_selection.lower())

        if "*" in selection:
            for model_tag in models_by_tag:
                if fnmatch.fnmatchcase(model_tag, selection):
                    matched_tags.add(model_tag)
        elif selection in models_by_tag:
            matched_tags.add(selection)

        if not matched_tags:
            logger.warning(f"Expression 'tag:{tag_selection}' doesn't match any models.")

        for tag in matched_tags:
            for model in models_by_tag[tag]:
                result.update(self._get_models(model, include_upstream, include_downstream, models))

        return result

    def _get_models(
        self,
        model_name: str,
        include_upstream: bool,
        include_downstream: bool,
        models: t.Dict[str, Model],
    ) -> t.Set[str]:
        result = {model_name}
        if include_upstream:
            result.update([u for u in self._dag.upstream(model_name) if u in models])
        if include_downstream:
            result.update(self._dag.downstream(model_name))
        return result

    @staticmethod
    def _get_value_and_dependency_inclusion(value: str) -> t.Tuple[str, bool, bool]:
        include_upstream = False
        include_downstream = False
        if value[0] == "+":
            value = value[1:]
            include_upstream = True
        if value[-1] == "+":
            value = value[:-1]
            include_downstream = True
        return value, include_upstream, include_downstream
