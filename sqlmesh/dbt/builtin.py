from __future__ import annotations

import json
import os
import typing as t
from ast import literal_eval

import agate
import jinja2
from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import Policy
from ruamel.yaml import YAMLError

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.adapter import ParsetimeAdapter, RuntimeAdapter
from sqlmesh.utils import AttributeDict, yaml
from sqlmesh.utils.errors import ConfigError, MacroEvalError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroReturnVal


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
    def var(name: str, default: t.Optional[str] = None) -> str:
        return variables.get(name, default)

    return var


def generate_ref(refs: t.Dict[str, t.Any]) -> t.Callable:

    # TODO suport package name
    def ref(package: str, name: t.Optional[str] = None) -> t.Optional[BaseRelation]:
        name = name or package
        relation_info = refs.get(name)
        if relation_info is None:
            return relation_info

        return BaseRelation.create(**relation_info, quote_policy=quote_policy())

    return ref


def generate_source(sources: t.Dict[str, t.Any]) -> t.Callable:
    def source(package: str, name: str) -> t.Optional[BaseRelation]:
        relation_info = sources.get(f"{package}.{name}")
        if relation_info is None:
            return relation_info

        return BaseRelation.create(**relation_info, quote_policy=quote_policy())

    return source


def quote_policy() -> Policy:
    return Policy(database=False, schema=False, identifier=False)


def return_val(val: t.Any) -> None:
    raise MacroReturnVal(val)


def to_set(value: t.Any, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return set(value)
    except TypeError:
        return default


def to_json(value: t.Any, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return json.dumps(value)
    except TypeError:
        return default


def from_json(value: str, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def to_yaml(value: t.Any, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return yaml.dumps(value)
    except (TypeError, YAMLError):
        return default


def from_yaml(value: t.Any, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return dict(yaml.load(value, raise_if_empty=False, render_jinja=False))
    except (TypeError, YAMLError):
        return default


def do_zip(*args: t.Any, default: t.Optional[t.Any] = None) -> t.Optional[t.Any]:
    try:
        return list(zip(*args))
    except TypeError:
        return default


def as_bool(value: str) -> bool:
    result = _try_literal_eval(value)
    if isinstance(result, bool):
        return result
    raise MacroEvalError(f"Failed to convert '{value}' into boolean.")


def as_number(value: str) -> t.Any:
    result = _try_literal_eval(value)
    if isinstance(value, (int, float)) and not isinstance(result, bool):
        return result
    raise MacroEvalError(f"Failed to convert '{value}' into number.")


def _try_literal_eval(value: str) -> t.Any:
    try:
        return literal_eval(value)
    except (ValueError, SyntaxError, MemoryError):
        return value


BUILTIN_GLOBALS = {
    "api": Api(),
    "config": config,
    "env_var": env_var,
    "exceptions": Exceptions(),
    "flags": Flags(),
    "fromjson": from_json,
    "fromyaml": from_yaml,
    "is_incremental": is_incremental,
    "log": no_log,
    "modules": Modules(),
    "print": no_log,
    "return": return_val,
    "set": to_set,
    "set_strict": set,
    "sqlmesh": True,
    "tojson": to_json,
    "toyaml": to_yaml,
    "zip": do_zip,
    "zip_strict": lambda *args: list(zip(*args)),
}

BUILTIN_FILTERS = {
    "as_bool": as_bool,
    "as_native": _try_literal_eval,
    "as_number": as_number,
    "as_text": lambda v: "" if v is None else str(v),
}


def create_builtin_globals(
    jinja_macros: JinjaMacroRegistry,
    jinja_globals: t.Dict[str, t.Any],
    engine_adapter: t.Optional[EngineAdapter],
) -> t.Dict[str, t.Any]:
    builtin_globals = BUILTIN_GLOBALS.copy()
    jinja_globals = jinja_globals.copy()

    this = jinja_globals.pop("this", None)
    if this is not None:
        if not isinstance(this, BaseRelation):
            builtin_globals["this"] = BaseRelation.create(**this, quote_policy=quote_policy())
        else:
            builtin_globals["this"] = this

    sources = jinja_globals.pop("sources", None)
    if sources is not None:
        builtin_globals["source"] = generate_source(sources)

    refs = jinja_globals.pop("refs", None)
    if refs is not None:
        builtin_globals["ref"] = generate_ref(refs)

    variables = jinja_globals.pop("vars", None)
    if variables is not None:
        builtin_globals["var"] = generate_var(variables)

    builtin_globals["builtins"] = AttributeDict(
        {k: builtin_globals.get(k) for k in ("ref", "source", "config")}
    )

    if engine_adapter is not None:
        adapter = RuntimeAdapter(
            engine_adapter, jinja_macros, jinja_globals={**builtin_globals, **jinja_globals}
        )
        sql_execution = SQLExecution(adapter)
        builtin_globals.update(
            {
                "execute": True,
                "adapter": adapter,
                "load_relation": lambda r: adapter.get_relation(r.database, r.schema, r.identifier),
                "store_result": sql_execution.store_result,
                "load_result": sql_execution.load_result,
                "run_query": sql_execution.run_query,
                "statement": sql_execution.statement,
                "log": log,
                "print": log,
            }
        )
    else:
        builtin_globals.update(
            {
                "execute": False,
                "adapter": ParsetimeAdapter(
                    jinja_macros, jinja_globals={**builtin_globals, **jinja_globals}
                ),
                "load_relation": lambda *args, **kwargs: None,
                "store_result": lambda *args, **kwargs: "",
                "load_result": lambda *args, **kwargs: None,
                "run_query": lambda *args, **kwargs: None,
                "statement": lambda *args, **kwargs: "",
                "log": no_log,
                "print": no_log,
            }
        )

    return {**builtin_globals, **jinja_globals}


def create_builtin_filters() -> t.Dict[str, t.Callable]:
    return BUILTIN_FILTERS
