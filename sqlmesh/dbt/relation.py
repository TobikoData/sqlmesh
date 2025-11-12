from sqlmesh.dbt.util import DBT_VERSION


if DBT_VERSION >= (1, 8, 0):
    from dbt.adapters.contracts.relation import *  # type: ignore # noqa: F403
else:
    from dbt.contracts.relation import *  # type: ignore  # noqa: F403
