from lsprotocol.types import Position
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import get_all_references, get_references, LSPModelReference
from sqlmesh.lsp.uri import URI
from tests.lsp.test_reference_cte import find_ranges_from_regex


def test_model_reference_with_column_prefix():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test finding references for "sushi.orders"
    ranges = find_ranges_from_regex(read_file, r"sushi\.orders")

    # Click on the table reference in FROM clause (should be the second occurrence)
    from_clause_range = None
    for r in ranges:
        line_content = read_file[r.start.line].strip()
        if "FROM" in line_content:
            from_clause_range = r
            break

    assert from_clause_range is not None, "Should find FROM clause with sushi.orders"

    position = Position(
        line=from_clause_range.start.line, character=from_clause_range.start.character + 6
    )

    model_refs = get_all_references(lsp_context, URI.from_path(sushi_customers_path), position)

    assert len(model_refs) >= 7

    # Verify that we have the FROM clause reference
    assert any(ref.range.start.line == from_clause_range.start.line for ref in model_refs), (
        "Should find FROM clause reference"
    )


def test_column_prefix_references_are_found():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Find all occurrences of sushi.orders in the file
    ranges = find_ranges_from_regex(read_file, r"sushi\.orders")

    # Should find exactly 2: FROM clause and WHERE clause with column prefix
    assert len(ranges) == 2, (
        f"Expected 2 occurrences of 'sushi.orders', found {len(ranges)}"
    )

    # Verify we have the expected lines
    line_contents = [read_file[r.start.line].strip() for r in ranges]

    # Should find FROM clause
    assert any("FROM sushi.orders" in content for content in line_contents), (
        "Should find FROM clause with sushi.orders"
    )

    # Should find customer_id in WHERE clause with column prefix
    assert any("WHERE sushi.orders.customer_id" in content for content in line_contents), (
        "Should find WHERE clause with sushi.orders.customer_id"
    )
