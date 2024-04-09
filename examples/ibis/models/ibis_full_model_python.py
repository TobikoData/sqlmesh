import typing as t
from datetime import datetime

import ibis  # type: ignore
import pandas as pd
from constants import DB_PATH  # type: ignore

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import FullKind


@model(
    "ibis.ibis_full_model_python",
    kind=FullKind(),
    columns={
        "item_id": "int",
        "num_orders": "int",
    },
    audits=["assert_positive_order_ids"],
    description="This model uses ibis to transform a `table` object and return a dataframe",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # get physical table name
    table_name = context.table("ibis.incremental_model").split(".")

    # connect ibis to database
    con = ibis.duckdb.connect(DB_PATH)

    # retrieve table
    incremental_model = con.table(name=table_name[-1], schema=table_name[-2])

    # build query
    count = incremental_model.id.nunique()
    aggregate = incremental_model.group_by("item_id").aggregate(num_orders=count)
    query = aggregate.order_by("item_id")

    return query.to_pandas()
