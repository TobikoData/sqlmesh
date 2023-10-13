from __future__ import annotations

import typing as t

from sqlmesh import Model
from sqlmesh.core.context import ExecutionContext
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.test.definition import _fully_qualified_test_fixture_name
from sqlmesh.utils import UniqueKeyDict


class TestExecutionContext(ExecutionContext):
    """The context needed to execute a Python model test.

    Args:
        engine_adapter: The engine adapter to execute queries against.
        models: All upstream models to use for expansion and mapping of physical locations.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        models: UniqueKeyDict[str, Model],
        default_dialect: t.Optional[str],
        default_catalog: t.Optional[str] = None,
    ):
        self.is_dev = True
        self._engine_adapter = engine_adapter
        self.__model_tables = {
            name: _fully_qualified_test_fixture_name(name, model.dialect)
            for name, model in models.items()
        }
        self._default_catalog = default_catalog
        self._default_dialect = default_dialect

    @property
    def _model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""
        return self.__model_tables
