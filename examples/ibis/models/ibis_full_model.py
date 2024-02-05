import ibis

from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


@model("ibis.ibis_full_model", is_sql=True, kind="FULL", audits=["assert_positive_order_ids"])
def entrypoint(evaluator: MacroEvaluator) -> str:
    # connect to database
    con = ibis.duckdb.connect(database="data/local.duckdb")

    # retrieve table
    incremental_model = con.table("incremental_model", schema="ibis")

    incremental_model_ordered = incremental_model.order_by("item_id")
    count = incremental_model_ordered.id.nunique()
    query = incremental_model_ordered.group_by("item_id").aggregate(num_orders=count)

    return ibis.to_sql(query)
