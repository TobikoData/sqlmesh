from __future__ import annotations

import typing as t
from pathlib import Path

from sqlmesh.core.environment import Environment
from sqlmesh.core.loader import update_model_schemas
from sqlmesh.core.model import Model
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import SQLMeshError


class ModelSelector:
    def __init__(
        self,
        state_reader: StateReader,
        models: UniqueKeyDict[str, Model],
        context_path: Path = Path("."),
    ):
        self._state_reader = state_reader
        self._models = models
        self._context_path = context_path

    def select(
        self,
        model_selections: t.Iterable[str],
        target_env_name: str,
        fallback_env_name: t.Optional[str] = None,
    ) -> UniqueKeyDict[str, Model]:
        """Given a set of selections returns models from the current state with names matching the
        selection while sourcing the remaining models from the target environment.

        Args:
            selected_models: A set of selections.
            target_env_name: The name of the target environment.

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

        # TODO: Support selection expressions.
        all_selected_models = set(model_selections)

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

        update_model_schemas(dag, models, self._context_path)

        return models
