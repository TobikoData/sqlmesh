from __future__ import annotations

import fnmatch
import logging
import typing as t
from collections import defaultdict
from pathlib import Path

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.environment import Environment
from sqlmesh.core.loader import update_model_schemas
from sqlmesh.core.model import Model
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG

logger = logging.getLogger(__name__)


class Selector:
    def __init__(
        self,
        state_reader: StateReader,
        models: UniqueKeyDict[str, Model],
        context_path: Path = Path("."),
        dag: t.Optional[DAG[str]] = None,
        default_catalog: t.Optional[str] = None,
        dialect: t.Optional[str] = None,
    ):
        self._state_reader = state_reader
        self._models = models
        self._context_path = context_path
        self._default_catalog = default_catalog
        self._dialect = dialect
        self.__models_by_tag: t.Optional[t.Dict[str, t.Set[str]]] = None

        if dag is None:
            self._dag: DAG[str] = DAG()
            for fqn, model in models.items():
                self._dag.add(fqn, model.depends_on)
        else:
            self._dag = dag

    @property
    def _models_by_tag(self) -> t.Dict[str, t.Set[str]]:
        if self.__models_by_tag is None:
            self.__models_by_tag = defaultdict(set)
            for model in self._models.values():
                for tag in model.tags:
                    self.__models_by_tag[tag.lower()].add(model.fqn)
        return self.__models_by_tag

    def select_models(
        self,
        model_selections: t.Iterable[str],
        target_env_name: str,
        fallback_env_name: t.Optional[str] = None,
    ) -> UniqueKeyDict[str, Model]:
        """Given a set of selections returns models from the current state with names matching the
        selection while sourcing the remaining models from the target environment.

        Args:
            model_selections: A set of selections.
            target_env_name: The name of the target environment.
            fallback_env_name: The name of the fallback environment that will be used if the target
                environment doesn't exist.

        Returns:
            A dictionary of models.
        """
        target_env = self._state_reader.get_environment(Environment.normalize_name(target_env_name))
        if not target_env and fallback_env_name:
            target_env = self._state_reader.get_environment(
                Environment.normalize_name(fallback_env_name)
            )

        env_models = (
            {
                s.name: s.model
                for s in self._state_reader.get_snapshots(
                    target_env.snapshots, hydrate_seeds=True
                ).values()
                if s.is_model
            }
            if target_env
            else {}
        )

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
        for fqn in all_model_fqns:
            model: t.Optional[Model] = None
            if fqn not in all_selected_models and fqn in env_models:
                # Unselected modified or added model.
                model = env_models[fqn]
            elif fqn in all_selected_models and fqn in self._models:
                # Selected modified or removed model.
                model = self._models[fqn]

            if model:
                # model.copy() can't be used here due to a cached state that can be a part of a model instance.
                if model.fqn in subdag:
                    model = type(model).parse_obj(model.dict(exclude={"mapping_schema"}))
                    dag.add(model.fqn, model.depends_on)
                models[model.fqn] = model

        update_model_schemas(dag, models, self._context_path)

        return models

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

    def _get_models(
        self, model_name: str, include_upstream: bool, include_downstream: bool
    ) -> t.Set[str]:
        result = {model_name}
        if include_upstream:
            result.update(self._dag.upstream(model_name))
        if include_downstream:
            result.update(self._dag.downstream(model_name))
        return result

    def _expand_model_tag(self, tag_selection: str) -> t.Set[str]:
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
            for model_tag in self._models_by_tag:
                if fnmatch.fnmatchcase(model_tag, selection):
                    matched_tags.add(model_tag)
        elif selection in self._models_by_tag:
            matched_tags.add(selection)

        if not matched_tags:
            logger.warning(f"Expression 'tag:{tag_selection}' doesn't match any models.")

        for tag in matched_tags:
            for model in self._models_by_tag[tag]:
                result.update(self._get_models(model, include_upstream, include_downstream))

        return result

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

        for selection in model_selections:
            if not selection:
                continue

            if selection.startswith("tag:"):
                results.update(self._expand_model_tag(selection[4:]))
                continue

            (
                selection,
                include_upstream,
                include_downstream,
            ) = self._get_value_and_dependency_inclusion(selection.lower())

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
                results.update(self._get_models(model_fqn, include_upstream, include_downstream))

        return results
