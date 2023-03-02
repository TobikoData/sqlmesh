from __future__ import annotations

import os
import typing as t

import agate
import jinja2
from dbt.exceptions import CompilationError

from sqlmesh.dbt.adapter import Adapter
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError


class ExceptionsJinja:
    """Implements the dbt "exceptions" jinja namespace."""

    def raise_compiler_error(self, msg: str) -> None:
        from dbt.exceptions import CompilationError

        raise CompilationError(msg)

    def warn(self, msg: str) -> str:
        print(msg)
        return ""


class Api:
    def __init__(self) -> None:
        from dbt.adapters.base.column import Column
        from dbt.adapters.base.relation import BaseRelation

        self.Relation = BaseRelation
        self.Column = Column


def env_var(name: str, default: t.Optional[str] = None) -> t.Optional[str]:
    if name not in os.environ and default is None:
        raise ConfigError(f"Missing environment variable '{name}'")
    return os.environ.get(name, default)


def is_incremental() -> bool:
    return False


def log(msg: str, info: bool = False) -> str:
    print(msg)
    return ""


def no_log(msg: str, info: bool = False) -> str:
    return ""


def config(*args: t.Any, **kwargs: t.Any) -> str:
    return ""


def generate_var(variables: t.Dict[str, t.Any]) -> t.Callable:
    DBT_VAR_MAPPING = variables.copy()

    def var(name: str, default: t.Optional[str] = None) -> str:
        return DBT_VAR_MAPPING.get(name, default)

    return var


def generate_ref(refs: t.Dict[str, str]) -> t.Callable:
    DBT_REF_MAPPING = refs.copy()

    # TODO suport package name
    def ref(package: str, name: t.Optional[str] = None) -> t.Optional[str]:
        name = name or package
        return DBT_REF_MAPPING.get(name)

    return ref


def generate_source(sources: t.Dict[str, str]) -> t.Callable:
    DBT_SOURCE_MAPPING = sources.copy()

    def source(package: str, name: str) -> t.Optional[str]:
        return DBT_SOURCE_MAPPING.get(f"{package}.{name}")

    return source


def generate_adapter(target: TargetConfig) -> Adapter:
    sqlmesh_config = target.to_sqlmesh()
    engine_adapter = sqlmesh_config.create_engine_adapter()
    return Adapter(engine_adapter=engine_adapter)


class SQLExecution:
    def __init__(self, adapter: Adapter):
        self.adapter = adapter
        self._results: t.Dict[str, AttributeDict] = {}

    def store_result(self, name: str, response: t.Any, agate_table: t.Optional[agate.Table]) -> str:
        from dbt.clients import agate_helper

        if agate_table is None:
            agate_table = agate_helper.empty_table()

        self._results[name] = AttributeDict(
            {
                "response": response,
                "data": agate_helper.as_matrix(agate_table),
                "table": agate_table,
            }
        )
        return ""

    def load_result(self, name: str) -> t.Optional[AttributeDict]:
        return self._results.get(name)

    def run_query(self, sql: str) -> agate.Table:
        self.statement("run_query_statement", fetch_result=True, auto_begin=False, caller=sql)
        resp = self.load_result("run_query_statement")
        assert resp is not None
        return resp["table"]

    def statement(
        self,
        name: t.Optional[str],
        fetch_result: bool = False,
        auto_begin: bool = True,
        language: str = "sql",
        caller: t.Optional[jinja2.runtime.Macro | str] = None,
    ) -> str:
        """
        Executes the SQL that is defined within the context of the caller. Therefore caller really isn't optional
        but we make it optional and at the end because we need to match the signature of the jinja2 macro.

        Name is the name that we store the results to which can be retrieved with `load_result`. If name is not
        provided then the SQL is executed but the results are not stored.
        """
        if not caller:
            raise RuntimeError(
                "Statement relies on a caller to be set that is the target SQL to be run"
            )
        sql = caller if isinstance(caller, str) else caller()
        if language != "sql":
            raise NotImplementedError(
                "SQLMesh's dbt integration only supports SQL statements at this time."
            )
        assert self.adapter is not None
        res, table = self.adapter.execute(sql, fetch=fetch_result, auto_begin=auto_begin)
        if name:
            self.store_result(name, res, table)
        return ""


BUILTIN_JINJA = {
    "env_var": env_var,
    "is_incremental": is_incremental,
    "log": no_log,
    "config": config,
    "sqlmesh": True,
    "exceptions": ExceptionsJinja(),
    "api": Api(),
}
