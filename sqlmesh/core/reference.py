from __future__ import annotations

import typing as t
from collections import defaultdict, deque

from sqlglot import exp

from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.model import Model


class Reference(PydanticModel, frozen=True):
    model_name: str
    expression: exp.Expression
    unique: bool = False
    _name: str = ""

    @property
    def columns(self) -> t.List[str]:
        expression = self.expression
        if isinstance(expression, exp.Alias):
            expression = expression.this
        expression = expression.unnest()
        if isinstance(expression, (exp.Array, exp.Tuple)):
            return [e.output_name for e in expression.expressions]
        return [expression.output_name]

    @property
    def name(self) -> str:
        if not self._name:
            keys = []

            if isinstance(self.expression, (exp.Tuple, exp.Array)):
                for e in self.expression.expressions:
                    if not e.output_name:
                        raise ConfigError(
                            f"Reference '{e}' must have an inferrable name or explicit alias."
                        )
                    keys.append(e.output_name)
            elif self.expression.output_name:
                keys.append(self.expression.output_name)
            else:
                raise ConfigError(
                    f"Reference '{self.expression}' must have an inferrable name or explicit alias."
                )

            self._name = "__".join(keys)
        return self._name


class ReferenceGraph:
    def __init__(self, models: t.Iterable[Model]):
        self._model_refs: t.DefaultDict[str, t.Dict[str, Reference]] = defaultdict(dict)
        self._ref_models: t.DefaultDict[str, t.Dict[str, str]] = defaultdict(dict)

        for model in models:
            self.add_model(model)

    def add_model(self, model: Model) -> None:
        for ref in model.all_references:
            self._model_refs[model.name][ref.name] = ref
            self._ref_models[ref.name][model.name] = model.name

    def find_path(self, source: str, target: str, max_depth: int = 3) -> t.List[Reference]:
        queue = deque(([ref] for ref in self._model_refs[source].values()))

        while queue:
            path = queue.popleft()
            visited = set()
            many = False

            for ref in path:
                visited.add(ref.model_name)
                many = many or not ref.unique

            ref_name = path[-1].name

            for model_name in self._ref_models[ref_name].values():
                for ref in self._model_refs[model_name].values():
                    # paths cannot have loops or contain many to many refs
                    if model_name in visited or (many and not ref.unique):
                        continue

                    new_path = path + [ref]

                    if model_name == target:
                        return new_path

                    if len(new_path) < max_depth:
                        queue.append(new_path)

        raise SQLMeshError(
            f"Cannot find path between '{source}' and '{target}'. Make sure that references/grains are configured and that a many to many join is not occurring."
        )
