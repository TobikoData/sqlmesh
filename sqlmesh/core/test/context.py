from __future__ import annotations

import typing as t
from functools import cached_property

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
        variables: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.is_dev = True
        self._engine_adapter = engine_adapter
        self._models = models
        self._default_catalog = default_catalog
        self._default_dialect = default_dialect
        self._variables = variables or {}

    @cached_property
    def _model_tables(self) -> t.Dict[str, str]:
        """Returns a mapping of model names to tables."""
        return {
            name: _fully_qualified_test_fixture_name(name, model.dialect)
            for name, model in self._models.items()
        }

    def with_variables(self, variables: t.Dict[str, t.Any]) -> TestExecutionContext:
        """Returns a new TestExecutionContext with additional variables."""
        return TestExecutionContext(
            self._engine_adapter,
            self._models,
            self._default_dialect,
            self._default_catalog,
            variables=variables,
        )
