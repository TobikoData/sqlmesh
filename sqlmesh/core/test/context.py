from __future__ import annotations

from sqlmesh.core.context import ExecutionContext
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.model import Model
from sqlmesh.core.test.definition import _test_fixture_name


class TestExecutionContext(ExecutionContext):
    """The context needed to execute a Python model test.

    Args:
        engine_adapter: The engine adapter to execute queries against.
        models: All upstream models to use for expansion and mapping of physical locations.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        models: dict[str, Model],
    ):
        self.is_dev = True
        self._engine_adapter = engine_adapter
        self.__model_tables = {k: _test_fixture_name(k) for k in models}

    @property
    def _model_tables(self) -> dict[str, str]:
        """Returns a mapping of model names to tables."""
        return self.__model_tables
