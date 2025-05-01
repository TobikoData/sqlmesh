import pytest
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext


@pytest.mark.fast
def test_lsp_context():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    assert lsp_context is not None
    assert lsp_context.context is not None
    assert lsp_context.map is not None

    # find one model in the map
    active_customers_key = next(
        key for key in lsp_context.map.keys() if key.endswith("models/active_customers.sql")
    )
    assert lsp_context.map[active_customers_key] == ["sushi.active_customers"]
