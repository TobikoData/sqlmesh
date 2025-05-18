import pytest
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget


@pytest.mark.fast
def test_lsp_context():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    assert lsp_context is not None
    assert lsp_context.context is not None
    assert lsp_context.map is not None

    # find one model in the map
    active_customers_key = next(
        key for key in lsp_context.map.keys() if str(key.to_path()).endswith("active_customers.sql")
    )

    # Check that the value is a ModelInfo with the expected model name
    assert isinstance(lsp_context.map[active_customers_key], ModelTarget)
    assert "sushi.active_customers" in lsp_context.map[active_customers_key].names

    # Check that all the values in the map are normalised
    keys = lsp_context.map.keys()
    for key in keys:
        stripped_key_from_prefix = str(key).split("file://")[1]
        assert ":" not in stripped_key_from_prefix
        assert "\\" not in stripped_key_from_prefix
