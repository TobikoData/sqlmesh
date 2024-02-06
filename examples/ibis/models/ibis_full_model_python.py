import typing as t
from datetime import datetime

import ibis  # type: ignore
import pandas as pd

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
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # connect ibis to database
    con = ibis.duckdb.connect(database="data/local.duckdb")

    # retrieve table
    incremental_model = con.table("incremental_model", schema="ibis")

    # build query
    count = incremental_model.id.nunique()
    aggregate = incremental_model.group_by("item_id").aggregate(num_orders=count)
    query = aggregate.order_by("item_id")

    # convert ibis table into dataframe
    df = query.to_pandas()

    return df
