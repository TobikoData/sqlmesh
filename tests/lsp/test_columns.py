from sqlmesh import Context
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.lsp.columns import get_columns_and_ranges_for_model
from sqlmesh.lsp.context import LSPContext


def test_get_columns_and_ranges_for_model():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    model = lsp_context.context.get_model("sushi.customers")
    if not isinstance(model, SqlModel):
        raise ValueError("Model is not a SqlModel")

    columns = get_columns_and_ranges_for_model(model)
    assert columns is not None

    assert len(columns) == 3
    assert columns[0].column_name == "customer_id"
    assert columns[0].description == "customer_id uniquely identifies customers"
    assert columns[0].data_type == "INT"
    assert columns[0].range is not None
    assert columns[0].range.start.line == 27
    assert columns[0].range.end.line == 27
    assert columns[1].column_name == "status"
    assert columns[1].description is None
    assert columns[2].column_name == "zip"
    assert columns[2].description is None
