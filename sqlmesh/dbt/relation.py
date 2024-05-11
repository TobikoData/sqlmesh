from sqlmesh.dbt.util import DBT_VERSION


if DBT_VERSION < (1, 8):
    from dbt.contracts.relation import *  # noqa: F403
else:
    from dbt.adapters.contracts.relation import *  # noqa: F403
