import typing as t
import functools
from sqlmesh.utils.jinja import JinjaMacroRegistry
from dbt.adapters.base.relation import BaseRelation
from sqlmesh.dbt.builtin import Api
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.errors import ConfigError
from dbt.adapters.base import BaseRelation
from sqlglot import exp

from dbt.adapters.base import BaseRelation


def migrated_ref(
    dbt_api: Api,
    database: t.Optional[str] = None,
    schema: t.Optional[str] = None,
    identifier: t.Optional[str] = None,
    version: t.Optional[int] = None,
    sqlmesh_model_name: t.Optional[str] = None,
) -> BaseRelation:
    if version:
        raise ValueError("dbt model versions are not supported in converted projects.")

    return dbt_api.Relation.create(database=database, schema=schema, identifier=identifier)


def migrated_source(
    dbt_api: Api,
    database: t.Optional[str] = None,
    schema: t.Optional[str] = None,
    identifier: t.Optional[str] = None,
) -> BaseRelation:
    return dbt_api.Relation.create(database=database, schema=schema, identifier=identifier)


def create_builtin_globals(
    jinja_macros: JinjaMacroRegistry,
    global_vars: t.Dict[str, t.Any],
    engine_adapter: t.Optional[EngineAdapter],
    *args: t.Any,
    **kwargs: t.Any,
) -> t.Dict[str, t.Any]:
    import sqlmesh.utils.jinja as sqlmesh_native_jinja
    import sqlmesh.dbt.builtin as sqlmesh_dbt_jinja

    # Capture dialect before the dbt builtins pops it
    dialect = global_vars.get("dialect")

    sqlmesh_native_globals = sqlmesh_native_jinja.create_builtin_globals(
        jinja_macros, global_vars, *args, **kwargs
    )

    if this_model := global_vars.get("this_model"):
        # create a DBT-compatible version of @this_model for {{ this }}
        if isinstance(this_model, str):
            if not dialect:
                raise ConfigError("No dialect?")

            # in audits, `this_model` is a SQL SELECT query that selects from the current table
            # elsewhere, it's a fqn string
            parsed: exp.Expression = exp.maybe_parse(this_model, dialect=dialect)

            table: t.Optional[exp.Table] = None
            if isinstance(parsed, exp.Column):
                table = exp.to_table(this_model, dialect=dialect)
            elif isinstance(parsed, exp.Query):
                table = parsed.find(exp.Table)
            else:
                raise ConfigError(f"Not sure how to handle this_model: {this_model}")

            if table:
                # sqlmesh_dbt_jinja.create_builtin_globals() will construct a Relation for {{ this }} based on the supplied dict
                global_vars["this"] = {
                    "database": table.catalog,
                    "schema": table.db,
                    "identifier": table.name,
                }

        else:
            raise ConfigError(f"Unhandled this_model type: {type(this_model)}")

    sqlmesh_dbt_globals = sqlmesh_dbt_jinja.create_builtin_globals(
        jinja_macros, global_vars, engine_adapter, *args, **kwargs
    )

    def source(dbt_api: Api, source_name: str, table_name: str) -> BaseRelation:
        # some source() calls cant be converted to __migrated_source() calls because they contain dynamic parameters
        # this is a fallback and will be wrong in some situations because `sources` in DBT can be aliased in config
        # TODO: maybe we migrate sources into the SQLMesh variables so we can look them up here?
        return dbt_api.Relation.create(database=source_name, identifier=table_name)

    def ref(dbt_api: Api, ref_name: str, package: t.Optional[str] = None) -> BaseRelation:
        # some ref() calls cant be converted to __migrated_ref() calls because they contain dynamic parameters
        raise NotImplementedError(
            f"Unable to resolve ref: {ref_name}. Please replace it with an actual model name or use a SQLMesh macro to generate dynamic model name."
        )

    dbt_compatibility_shims = {
        "dialect": dialect,
        "__migrated_ref": functools.partial(migrated_ref, sqlmesh_dbt_globals["api"]),
        "__migrated_source": functools.partial(migrated_source, sqlmesh_dbt_globals["api"]),
        "source": functools.partial(source, sqlmesh_dbt_globals["api"]),
        "ref": functools.partial(ref, sqlmesh_dbt_globals["api"]),
        # make {{ config(...) }} a no-op, some macros call it but its meaningless in a SQLMesh Native project
        "config": lambda *_args, **_kwargs: None,
    }

    return {**sqlmesh_native_globals, **sqlmesh_dbt_globals, **dbt_compatibility_shims}
