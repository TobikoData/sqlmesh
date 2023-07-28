from __future__ import annotations

import json
import logging
import os
import typing as t
from ast import literal_eval
from dataclasses import asdict

import agate
import jinja2
from dbt import version
from dbt.adapters.base import BaseRelation, Column
from dbt.contracts.relation import Policy
from ruamel.yaml import YAMLError

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.dbt.adapter import BaseAdapter, ParsetimeAdapter, RuntimeAdapter
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils import AttributeDict, yaml
from sqlmesh.utils.errors import ConfigError, MacroEvalError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroReturnVal

logger = logging.getLogger(__name__)


class Exceptions:
    def raise_compiler_error(self, msg: str) -> None:
        if DBT_VERSION >= (1, 4):
            from dbt.exceptions import CompilationError

            raise CompilationError(msg)
        else:
            from dbt.exceptions import CompilationException  # type: ignore

            raise CompilationException(msg)

    def warn(self, msg: str) -> str:
        print(msg)
        return ""


class Api:
    def __init__(self, target: t.Optional[AttributeDict] = None) -> None:
        if target:
            config = TargetConfig.load(target)
            self.Relation = config.relation_class
            self.Column = config.column_class
            self.quote_policy = config.quote_policy
        else:
            self.Relation = BaseRelation
            self.Column = Column
            self.quote_policy = Policy()


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
    def __init__(self, adapter: BaseAdapter):
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
        res, table = self.adapter.execute(sql, fetch=fetch_result, auto_begin=auto_begin)
        if name:
            self.store_result(name, res, table)
        return ""


def env_var(name: str, default: t.Optional[str] = None) -> t.Optional[str]:
    if name not in os.environ and default is None:
        raise ConfigError(f"Missing environment variable '{name}'")
    return os.environ.get(name, default)


def log(msg: str, info: bool = False) -> str:
    print(msg)
    return ""


def no_log(msg: str, info: bool = False) -> str:
    return ""


def generate_var(variables: t.Dict[str, t.Any]) -> t.Callable:
    def var(name: str, default: t.Optional[t.Any] = None) -> t.Any:
        return variables.get(name, default)

    return var


def generate_ref(refs: t.Dict[str, t.Any], api: Api) -> t.Callable:
    def ref(package: str, name: t.Optional[str] = None) -> t.Optional[BaseRelation]:
        ref_name = f"{package}.{name}" if name else package
        relation_info = refs.get(ref_name)
        if relation_info is None:
            logger.debug("Could not resolve ref '%s'", ref_name)
            return None

        return _relation_info_to_relation(relation_info, api.Relation, api.quote_policy)

    return ref


def generate_source(sources: t.Dict[str, t.Any], api: Api) -> t.Callable:
    def source(package: str, name: str) -> t.Optional[BaseRelation]:
        relation_info = sources.get(f"{package}.{name}")
        if relation_info is None:
            logger.debug("Could not resolve source package='%s' name='%s'", package, name)
            return None

        return _relation_info_to_relation(relation_info, api.Relation, api.quote_policy)

    return source


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
        return yaml.dump(value)
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
    "dbt_version": version.__version__,
    "env_var": env_var,
    "exceptions": Exceptions(),
    "flags": Flags(),
    "fromjson": from_json,
    "fromyaml": from_yaml,
    "log": no_log,
    "modules": Modules(),
    "print": no_log,
    "return": return_val,
    "set": to_set,
    "set_strict": set,
    "sqlmesh": True,
    "sqlmesh_incremental": True,
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

    target: t.Optional[AttributeDict] = jinja_globals.get("target", None)
    api = Api(target)
    dialect = target.type if target else None  # type: ignore

    builtin_globals["api"] = api

    this = jinja_globals.pop("this", None)
    if this is not None:
        if not isinstance(this, api.Relation):
            builtin_globals["this"] = api.Relation.create(**this)
        else:
            builtin_globals["this"] = this

    sources = jinja_globals.pop("sources", None)
    if sources is not None:
        builtin_globals["source"] = generate_source(sources, api)

    refs = jinja_globals.pop("refs", None)
    if refs is not None:
        builtin_globals["ref"] = generate_ref(refs, api)

    variables = jinja_globals.pop("vars", None)
    if variables is not None:
        builtin_globals["var"] = generate_var(variables)

    is_incremental = jinja_globals.pop("has_intervals", False)
    builtin_globals["is_incremental"] = lambda: is_incremental

    builtin_globals["builtins"] = AttributeDict(
        {k: builtin_globals.get(k) for k in ("ref", "source", "config")}
    )

    if engine_adapter is not None:
        adapter: BaseAdapter = RuntimeAdapter(
            engine_adapter,
            jinja_macros,
            jinja_globals={
                **builtin_globals,
                **jinja_globals,
                "engine_adapter": engine_adapter,
            },
            relation_type=api.Relation,
            quote_policy=api.quote_policy,
            snapshots=jinja_globals.get("snapshots", {}),
            table_mapping=jinja_globals.get("table_mapping", {}),
            is_dev=jinja_globals.get("is_dev", False),
        )
        builtin_globals.update({"log": log, "print": log})
    else:
        adapter = ParsetimeAdapter(
            jinja_macros,
            jinja_globals={**builtin_globals, **jinja_globals},
            dialect=dialect,
        )
        builtin_globals.update({"log": no_log, "print": no_log})

    sql_execution = SQLExecution(adapter)
    builtin_globals.update(
        {
            "adapter": adapter,
            "execute": True,
            "load_relation": lambda r: adapter.get_relation(r.database, r.schema, r.identifier),
            "store_result": sql_execution.store_result,
            "load_result": sql_execution.load_result,
            "run_query": sql_execution.run_query,
            "statement": sql_execution.statement,
        }
    )

    return {**builtin_globals, **jinja_globals}


def create_builtin_filters() -> t.Dict[str, t.Callable]:
    return BUILTIN_FILTERS


def _relation_info_to_relation(
    relation_info: t.Dict[str, t.Any],
    relation_type: t.Type[BaseRelation],
    target_quote_policy: Policy,
) -> BaseRelation:
    relation_info = relation_info.copy()
    quote_policy = Policy(
        **{
            **asdict(target_quote_policy),
            **{k: v for k, v in relation_info.pop("quote_policy", {}).items() if v is not None},
        }
    )
    return relation_type.create(**relation_info, quote_policy=quote_policy)
