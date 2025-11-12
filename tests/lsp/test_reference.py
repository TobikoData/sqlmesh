from sqlmesh.core.context import Context
from sqlmesh.core.linter.rule import Position
from sqlmesh.lsp.context import LSPContext, ModelTarget, AuditTarget
from sqlmesh.lsp.reference import ModelReference, get_model_definitions_for_a_path, by_position
from sqlmesh.lsp.uri import URI


def test_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    active_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.active_customers" in info.names
    )
    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    active_customers_uri = URI.from_path(active_customers_path)
    references = get_model_definitions_for_a_path(lsp_context, active_customers_uri)

    assert len(references) == 1
    path = references[0].path
    assert path is not None
    assert path == sushi_customers_path

    # Check that the reference in the correct range is sushi.customers
    path = active_customers_uri.to_path()
    with open(path, "r") as file:
        read_file = file.readlines()

    # Get the string range in the read file
    referenced_text = get_string_from_range(read_file, references[0].range)
    assert referenced_text == "sushi.customers"


def test_reference_with_alias() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    waiter_revenue_by_day_path = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.waiter_revenue_by_day" in info.names
    )

    references = [
        ref
        for ref in get_model_definitions_for_a_path(
            lsp_context, URI.from_path(waiter_revenue_by_day_path)
        )
        if isinstance(ref, ModelReference)
    ]
    assert len(references) == 3

    with open(waiter_revenue_by_day_path, "r") as file:
        read_file = file.readlines()

    assert str(references[0].path).endswith("orders.py")
    assert get_string_from_range(read_file, references[0].range) == "sushi.orders"
    assert (
        references[0].markdown_description
        == """Table of sushi orders.

| Column | Type | Description |
|--------|------|-------------|
| id | INT |  |
| customer_id | INT |  |
| waiter_id | INT |  |
| start_ts | INT |  |
| end_ts | INT |  |
| event_date | DATE |  |"""
    )
    assert str(references[1].path).endswith("order_items.py")
    assert get_string_from_range(read_file, references[1].range) == "sushi.order_items"
    assert str(references[2].path).endswith("items.py")
    assert get_string_from_range(read_file, references[2].range) == "sushi.items"


def test_standalone_audit_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the standalone audit URI
    audit_path = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, AuditTarget) and info.name == "assert_item_price_above_zero"
    )
    # Find the items model URI
    items_path = next(
        uri
        for uri, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.items" in info.names
    )

    references = get_model_definitions_for_a_path(lsp_context, URI.from_path(audit_path))

    assert len(references) == 1
    assert references[0].path == items_path

    # Check that the reference in the correct range is sushi.items
    with open(audit_path, "r") as file:
        read_file = file.readlines()
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


def test_filter_references_by_position() -> None:
    """Test that we can filter references correctly based on cursor position."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Use a file with multiple references (waiter_revenue_by_day)
    waiter_revenue_by_day_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.waiter_revenue_by_day" in info.names
    )

    # Get all references in the file
    all_references = get_model_definitions_for_a_path(
        lsp_context, URI.from_path(waiter_revenue_by_day_path)
    )
    assert len(all_references) == 3

    # Get file contents to locate positions for testing
    with open(waiter_revenue_by_day_path, "r") as file:
        read_file = file.readlines()

    # Test positions for each reference
    for i, reference in enumerate(all_references):
        # Position inside the reference - should return exactly one reference
        middle_line = (reference.range.start.line + reference.range.end.line) // 2
        middle_char = (reference.range.start.character + reference.range.end.character) // 2
        position_inside = Position(line=middle_line, character=middle_char)
        filtered = list(filter(by_position(position_inside), all_references))
        assert len(filtered) == 1
        assert filtered[0].path == reference.path
        assert filtered[0].range == reference.range

        # For testing outside position, use a position before the current reference
        # or after the last reference for the last one
        if i == 0:
            outside_line = reference.range.start.line
            outside_char = max(0, reference.range.start.character - 5)
        else:
            prev_ref = all_references[i - 1]
            outside_line = prev_ref.range.end.line
            outside_char = prev_ref.range.end.character + 5

        position_outside = Position(line=outside_line, character=outside_char)
        filtered_outside = list(filter(by_position(position_outside), all_references))
        assert reference not in filtered_outside, (
            f"Reference {i} should not match position outside its range"
        )

    # Test case: cursor at beginning of file - no references should match
    position_start = Position(line=0, character=0)
    filtered_start = list(filter(by_position(position_start), all_references))
    assert len(filtered_start) == 0 or all(
        ref.range.start.line == 0 and ref.range.start.character <= 0 for ref in filtered_start
    )

    # Test case: cursor at end of file - no references should match (unless there's a reference at the end)
    last_line = len(read_file) - 1
    last_char = len(read_file[last_line]) - 1
    position_end = Position(line=last_line, character=last_char)
    filtered_end = list(filter(by_position(position_end), all_references))
    assert len(filtered_end) == 0 or all(
        ref.range.end.line >= last_line and ref.range.end.character >= last_char
        for ref in filtered_end
    )
