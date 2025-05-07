import pytest
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.reference import get_model_definitions_for_a_path


@pytest.mark.fast
def test_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    active_customers_uri = next(
        uri for uri, models in lsp_context.map.items() if "sushi.active_customers" in models
    )
    sushi_customers_uri = next(
        uri for uri, models in lsp_context.map.items() if "sushi.customers" in models
    )

    references = get_model_definitions_for_a_path(lsp_context, active_customers_uri)

    assert len(references) == 1
    assert references[0].uri == sushi_customers_uri

    # Check that the reference in the correct range is sushi.customers
    path = active_customers_uri.removeprefix("file://")
    read_file = open(path, "r").readlines()
    # Get the string range in the read file
    reference_range = references[0].range
    start_line = reference_range.start.line
    end_line = reference_range.end.line
    start_character = reference_range.start.character
    end_character = reference_range.end.character
    # Get the string from the file

    # If the reference spans multiple lines, handle it accordingly
    if start_line == end_line:
        # Reference is on a single line
        line_content = read_file[start_line]
        referenced_text = line_content[start_character:end_character]
    else:
        # Reference spans multiple lines
        referenced_text = read_file[start_line][
            start_character:
        ]  # First line from start_character to end
        for line_num in range(start_line + 1, end_line):  # Middle lines (if any)
            referenced_text += read_file[line_num]
        referenced_text += read_file[end_line][:end_character]  # Last line up to end_character
    assert referenced_text == "sushi.customers"


@pytest.mark.fast
def test_reference_with_alias() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    waiter_revenue_by_day_uri = next(
        uri for uri, models in lsp_context.map.items() if "sushi.waiter_revenue_by_day" in models
    )

    references = get_model_definitions_for_a_path(lsp_context, waiter_revenue_by_day_uri)
    assert len(references) == 3

    assert references[0].uri.endswith("orders.py")
    assert references[1].uri.endswith("order_items.py")
    assert references[2].uri.endswith("items.py")
