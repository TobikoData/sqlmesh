from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget


def test_lsp_context():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    assert lsp_context is not None
    assert lsp_context.context is not None
    assert lsp_context.map is not None

    # find one model in the map
    active_customers_key = next(
        key for key in lsp_context.map.keys() if key.name == "active_customers.sql"
    )

    # Check that the value is a ModelInfo with the expected model name
    assert isinstance(lsp_context.map[active_customers_key], ModelTarget)
    assert "sushi.active_customers" in lsp_context.map[active_customers_key].names
