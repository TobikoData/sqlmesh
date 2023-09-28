from __future__ import annotations

import fnmatch
import typing as t
from pathlib import Path

from sqlmesh.core.config import ModelDefaultsConfig
from sqlmesh.core.environment import Environment
from sqlmesh.core.loader import update_model_schemas
from sqlmesh.core.model import Model
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import SQLMeshError


class Selector:
    def __init__(
        self,
        state_reader: StateReader,
        models: UniqueKeyDict[str, Model],
        model_defaults: t.Dict[str, ModelDefaultsConfig],
        context_path: Path = Path("."),
        dag: t.Optional[DAG[str]] = None,
    ):
        self._state_reader = state_reader
        self._models = models
        self._context_path = context_path
        self._model_defaults = model_defaults

        if dag is None:
            self._dag: DAG[str] = DAG()
            for model in models.values():
                self._dag.add(model.name, model.depends_on)
        else:
            self._dag = dag

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
        if not target_env:
            raise SQLMeshError(
                f"Either the '{target_env_name}' or the '{fallback_env_name}' environment must exist in order to apply model selection."
            )

        env_models = {
            s.name: s.model
            for s in self._state_reader.get_snapshots(
                target_env.snapshots, hydrate_seeds=True
            ).values()
        }

        all_selected_models = self.expand_model_selections(model_selections)

        dag: DAG[str] = DAG()
        models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
        all_model_names = set(self._models) | set(env_models)
        for name in all_model_names:
            model: t.Optional[Model] = None
            if name not in all_selected_models and name in env_models:
                # Unselected modified or added model.
                model = env_models[name]
            elif name in all_selected_models and name in self._models:
                # Selected modified or removed model.
                model = self._models[name]

            if model:
                # model.copy() can't be used here due to a cached state that can be a part of a model instance.
                model = type(model).parse_obj(model.dict(exclude={"mapping_schema"}))
                models[name] = model
                dag.add(model.name, model.depends_on)

        update_model_schemas(dag, models, self._context_path, self._model_defaults)

        return models

    def expand_model_selections(self, model_selections: t.Iterable[str]) -> t.Set[str]:
        """Expands a set of model selections into a set of model names.

        Args:
            model_selections: A set of model selections.

        Returns:
            A set of model names.
        """
        result: t.Set[str] = set()

        def _add_model(model_name: str, include_upstream: bool, include_downstream: bool) -> None:
            result.add(model_name)
            if include_upstream:
                result.update(self._dag.upstream(model_name))
            if include_downstream:
                result.update(self._dag.downstream(model_name))

        for selection in model_selections:
            if not selection:
                continue

            include_upstream = False
            include_downstream = False
            if selection[0] == "+":
                selection = selection[1:]
                include_upstream = True
            if selection[-1] == "+":
                selection = selection[:-1]
                include_downstream = True

            if "*" in selection:
                for name in self._models:
                    if fnmatch.fnmatch(name, selection):
                        _add_model(name, include_upstream, include_downstream)
            else:
                _add_model(selection, include_upstream, include_downstream)

        return result
