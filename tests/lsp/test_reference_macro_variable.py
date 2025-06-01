import pytest
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import get_macro_definitions_for_a_path, get_references, get_macro_variables_definitions
from sqlmesh.lsp.uri import URI


@pytest.mark.fast
def test_macro_references() -> None:
    """Test that macro references (e.g., @ADD_ONE, @MULTIPLY) have proper go-to-definition support."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the top_waiters model that uses macros
    waiter_revenue_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.waiter_revenue_by_day" in info.names
    )
    waiter_revenue_uri = URI.from_path(waiter_revenue_path)
    macro_references = get_macro_variables_definitions(lsp_context, waiter_revenue_uri)

    # We expect 3 macro references: @ADD_ONE, @MULTIPLY, @SQL_LITERAL
    assert len(macro_references) == 2

    # Check that all references point to the utils.py file
    for ref in macro_references:
        assert ref.uri.endswith("sushi/macros/utils.py")
        assert ref.target_range is not None
