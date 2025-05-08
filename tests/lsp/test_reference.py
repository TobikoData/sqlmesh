import pytest
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget, AuditTarget
from sqlmesh.lsp.reference import get_model_definitions_for_a_path


@pytest.mark.fast
def test_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    active_customers_uri = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.active_customers" in info.names
    )
    sushi_customers_uri = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    references = get_model_definitions_for_a_path(lsp_context, active_customers_uri)

    assert len(references) == 1
    assert references[0].uri == sushi_customers_uri

    # Check that the reference in the correct range is sushi.customers
    path = active_customers_uri.removeprefix("file://")
    read_file = open(path, "r").readlines()
    # Get the string range in the read file
    referenced_text = get_string_from_range(read_file, references[0].range)
    assert referenced_text == "sushi.customers"


@pytest.mark.fast
def test_reference_with_alias() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    waiter_revenue_by_day_uri = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.waiter_revenue_by_day" in info.names
    )

    references = get_model_definitions_for_a_path(lsp_context, waiter_revenue_by_day_uri)
    assert len(references) == 3

    path = waiter_revenue_by_day_uri.removeprefix("file://")
    read_file = open(path, "r").readlines()

    assert references[0].uri.endswith("orders.py")
    assert get_string_from_range(read_file, references[0].range) == "sushi.orders"
    assert references[1].uri.endswith("order_items.py")
    assert get_string_from_range(read_file, references[1].range) == "sushi.order_items"
    assert references[2].uri.endswith("items.py")
    assert get_string_from_range(read_file, references[2].range) == "sushi.items"


@pytest.mark.fast
def test_standalone_audit_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the standalone audit URI
    audit_uri = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, AuditTarget) and info.name == "assert_item_price_above_zero"
    )

    # Find the items model URI
    items_uri = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.items" in info.names
    )

    references = get_model_definitions_for_a_path(lsp_context, audit_uri)

    assert len(references) == 1
    assert references[0].uri == items_uri

    # Check that the reference in the correct range is sushi.items
    path = audit_uri.removeprefix("file://")
    read_file = open(path, "r").readlines()
    referenced_text = get_string_from_range(read_file, references[0].range)
    assert referenced_text == "sushi.items"


def get_string_from_range(file_lines, range_obj) -> str:
    start_line = range_obj.start.line
    end_line = range_obj.end.line
    start_character = range_obj.start.character
    end_character = range_obj.end.character

    # If the reference spans multiple lines, handle it accordingly
    if start_line == end_line:
        # Reference is on a single line
        line_content = file_lines[start_line]
        return line_content[start_character:end_character]

    # Reference spans multiple lines
    result = file_lines[start_line][start_character:]  # First line from start_character to end
    for line_num in range(start_line + 1, end_line):  # Middle lines (if any)
        result += file_lines[line_num]
    result += file_lines[end_line][:end_character]  # Last line up to end_character
    return result
