from lsprotocol.types import Position
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.rename import prepare_rename, rename_symbol
from sqlmesh.lsp.uri import URI
from tests.lsp.test_reference_cte import find_ranges_from_regex


def test_prepare_rename_cte():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test clicking on CTE definition for "current_marketing"
    ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(ranges) == 2

    # Click on the CTE definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    result = prepare_rename(lsp_context, URI.from_path(sushi_customers_path), position)

    assert result is not None
    assert result.placeholder == "cte_name"
    assert result.range == ranges[0]  # Should return the definition range

    # Test clicking on CTE usage
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    result = prepare_rename(lsp_context, URI.from_path(sushi_customers_path), position)

    assert result is not None
    assert result.placeholder == "cte_name"
    assert result.range == ranges[0]  # Should still return the definition range


def test_prepare_rename_cte_outer():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test clicking on CTE definition for "current_marketing_outer"
    ranges = find_ranges_from_regex(read_file, r"current_marketing_outer")
    assert len(ranges) == 2

    # Click on the CTE definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    result = prepare_rename(lsp_context, URI.from_path(sushi_customers_path), position)

    assert result is not None
    assert result.placeholder == "cte_name"
    assert result.range == ranges[0]  # Should return the definition range


def test_prepare_rename_non_cte():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Click on a regular table reference (not a CTE)
    ranges = find_ranges_from_regex(read_file, r"sushi\.orders")
    assert len(ranges) >= 1

    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    result = prepare_rename(lsp_context, URI.from_path(sushi_customers_path), position)

    assert result is None


def test_rename_cte():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test renaming "current_marketing" to "new_marketing"
    ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(ranges) == 2

    # Click on the CTE definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    workspace_edit = rename_symbol(
        lsp_context, URI.from_path(sushi_customers_path), position, "new_marketing"
    )

    assert workspace_edit is not None
    assert workspace_edit.changes is not None

    uri = URI.from_path(sushi_customers_path).value
    assert uri in workspace_edit.changes

    edits = workspace_edit.changes[uri]

    # Should have edited four occurences including column usages
    assert len(edits) == 4

    # Verify that both ranges are being edited
    edit_ranges = [edit.range for edit in edits]
    for expected_range in ranges:
        assert any(
            edit_range.start.line == expected_range.start.line
            and edit_range.start.character == expected_range.start.character
            for edit_range in edit_ranges
        ), (
            f"Expected to find edit at line {expected_range.start.line}, char {expected_range.start.character}"
        )

    # Verify that all edits have the new name
    assert all(edit.new_text == "new_marketing" for edit in edits)

    # Apply the edits to verify the result
    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    # Apply edits in reverse order to avoid offset issues
    sorted_edits = sorted(
        edits, key=lambda e: (e.range.start.line, e.range.start.character), reverse=True
    )
    for edit in sorted_edits:
        line_idx = edit.range.start.line
        start_char = edit.range.start.character
        end_char = edit.range.end.character

        line = lines[line_idx]
        new_line = line[:start_char] + edit.new_text + line[end_char:]
        lines[line_idx] = new_line

    # Verify the edited content
    edited_content = "".join(lines)
    assert "new_marketing" in edited_content
    assert "current_marketing" not in edited_content.replace("current_marketing_outer", "")
    assert edited_content.count("new_marketing") == 4
    assert (
        "  SELECT new_marketing.* FROM new_marketing WHERE new_marketing.customer_id != 100\n"
        in lines
    )


def test_rename_cte_outer():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test renaming "current_marketing_outer" to "new_marketing_outer"
    ranges = find_ranges_from_regex(read_file, r"current_marketing_outer")
    assert len(ranges) == 2

    # Click on the CTE usage
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    workspace_edit = rename_symbol(
        lsp_context, URI.from_path(sushi_customers_path), position, "new_marketing_outer"
    )

    assert workspace_edit is not None
    assert workspace_edit.changes is not None

    uri = URI.from_path(sushi_customers_path).value
    assert uri in workspace_edit.changes

    edits = workspace_edit.changes[uri]
    assert len(edits) == 2  # Should have 2 edits: definition + usage

    # Verify that both ranges are being edited
    edit_ranges = [edit.range for edit in edits]
    for expected_range in ranges:
        assert any(
            edit_range.start.line == expected_range.start.line
            and edit_range.start.character == expected_range.start.character
            for edit_range in edit_ranges
        ), (
            f"Expected to find edit at line {expected_range.start.line}, char {expected_range.start.character}"
        )

    # Verify that all edits have the new name
    assert all(edit.new_text == "new_marketing_outer" for edit in edits)
