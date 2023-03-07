from __future__ import annotations

import os
import typing as t

import agate
import jinja2
from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import Policy

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.adapter import ParsetimeAdapter, RuntimeAdapter
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroReturnVal

if t.TYPE_CHECKING:
    from sqlmesh.dbt.model import ModelConfig
    from sqlmesh.dbt.seed import SeedConfig
    from sqlmesh.dbt.source import SourceConfig


class Exceptions:
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


class Flags:
    def __init__(self) -> None:
        # Temporary placeholder values for now (these are generally passed from the CLI)
        self.FULL_REFRESH = None
        self.STORE_FAILURES = None
        self.WHICH = None


class Modules:
    def __init__(self) -> None:
        import datetime
        import itertools
        import re

        try:
            import pytz

            self.pytz = pytz
        except ImportError:
            pass

        self.datetime = datetime
        self.re = re
        self.itertools = itertools


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


def generate_ref(refs: t.Dict[str, t.Union[ModelConfig, SeedConfig]]) -> t.Callable:
    DBT_REF_MAPPING = {k: v.relation_info for k, v in refs.items()}

    # TODO suport package name
    def ref(package: str, name: t.Optional[str] = None) -> t.Optional[BaseRelation]:
        name = name or package
        relation_info = DBT_REF_MAPPING.get(name)
        if relation_info is None:
            return relation_info

        return BaseRelation.create(**relation_info, quote_policy=quote_policy())

    return ref


def generate_source(sources: t.Dict[str, SourceConfig]) -> t.Callable:
    DBT_SOURCE_MAPPING = {k: v.relation_info for k, v in sources.items()}

    def source(package: str, name: str) -> t.Optional[BaseRelation]:
        relation_info = DBT_SOURCE_MAPPING.get(f"{package}.{name}")
        if relation_info is None:
            return relation_info

        return BaseRelation.create(**relation_info, quote_policy=quote_policy())

    return source


def generate_this(model: ModelConfig) -> t.Callable:
    DBT_THIS_RELATION = model.relation_info

    def maybw() -> BaseRelation:
        return BaseRelation.create(**DBT_THIS_RELATION, quote_policy=quote_policy())

    return maybw


def quote_policy() -> Policy:
    return Policy(database=False, schema=False, identifier=False)


def return_val(val: t.Any) -> None:
    raise MacroReturnVal(val)


class SQLExecution:
    def __init__(self, adapter: RuntimeAdapter):
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
    "api": Api(),
    "config": config,
    "env_var": env_var,
    "exceptions": Exceptions(),
    "flags": Flags(),
    "is_incremental": is_incremental,
    "log": no_log,
    "modules": Modules(),
    "return": return_val,
    "sqlmesh": True,
}


def create_builtins(
    jinja_macros: JinjaMacroRegistry,
    jinja_globals: t.Dict[str, t.Any],
    engine_adapter: t.Optional[EngineAdapter],
) -> t.Dict[str, t.Any]:
    builtins = {
        **BUILTIN_JINJA,
        **jinja_globals,
    }

    if engine_adapter is not None:
        adapter = RuntimeAdapter(engine_adapter, jinja_macros, jinja_globals=builtins)
        sql_execution = SQLExecution(adapter)
        builtins.update(
            {
                "execute": True,
                "adapter": adapter,
                "load_relation": lambda r: adapter.get_relation(r.database, r.schema, r.identifier),
                "store_result": sql_execution.store_result,
                "load_result": sql_execution.load_result,
                "run_query": sql_execution.run_query,
                "statement": sql_execution.statement,
                "log": log,
            }
        )
    else:
        builtins.update(
            {
                "execute": False,
                "adapter": ParsetimeAdapter(jinja_macros, jinja_globals=builtins),
                "load_relation": lambda *args, **kwargs: None,
                "store_result": lambda *args, **kwargs: "",
                "load_result": lambda *args, **kwargs: None,
                "run_query": lambda *args, **kwargs: None,
                "statement": lambda *args, **kwargs: "",
                "log": no_log,
            }
        )

    return {**builtins, **jinja_globals}
