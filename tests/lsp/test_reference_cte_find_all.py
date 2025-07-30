from lsprotocol.types import Position
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import get_cte_references
from sqlmesh.lsp.uri import URI
from tests.lsp.test_reference_cte import find_ranges_from_regex


def test_cte_find_all_references():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test finding all references of "current_marketing"
    ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(ranges) == 2  # regex finds 2 occurrences (definition and FROM clause)

    # Click on the CTE definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    references = get_cte_references(lsp_context, URI.from_path(sushi_customers_path), position)
    # Should find the definition, FROM clause, and column prefix usages
    assert len(references) == 4  # definition + FROM + 2 column prefix uses
    assert all(ref.path == sushi_customers_path for ref in references)

    reference_ranges = [ref.range for ref in references]
    for expected_range in ranges:
        assert any(
            ref_range.start.line == expected_range.start.line
            and ref_range.start.character == expected_range.start.character
            for ref_range in reference_ranges
        ), (
            f"Expected to find reference at line {expected_range.start.line}, char {expected_range.start.character}"
        )

    # Click on the CTE usage
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    references = get_cte_references(lsp_context, URI.from_path(sushi_customers_path), position)

    # Should find the same references
    assert len(references) == 4  # definition + FROM + 2 column prefix uses
    assert all(ref.path == sushi_customers_path for ref in references)

    reference_ranges = [ref.range for ref in references]
    for expected_range in ranges:
        assert any(
            ref_range.start.line == expected_range.start.line
            and ref_range.start.character == expected_range.start.character
            for ref_range in reference_ranges
        ), (
            f"Expected to find reference at line {expected_range.start.line}, char {expected_range.start.character}"
        )


def test_cte_find_all_references_outer():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Test finding all references of "current_marketing_outer"
    ranges = find_ranges_from_regex(read_file, r"current_marketing_outer")
    assert len(ranges) == 2

    # Click on the CTE definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    references = get_cte_references(lsp_context, URI.from_path(sushi_customers_path), position)

    # Should find both the definition and the usage
    assert len(references) == 2
    assert all(ref.path == sushi_customers_path for ref in references)

    # Verify that we found both occurrences
    reference_ranges = [ref.range for ref in references]
    for expected_range in ranges:
        assert any(
            ref_range.start.line == expected_range.start.line
            and ref_range.start.character == expected_range.start.character
            for ref_range in reference_ranges
        ), (
            f"Expected to find reference at line {expected_range.start.line}, char {expected_range.start.character}"
        )

    # Click on the CTE usage
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    references = get_cte_references(lsp_context, URI.from_path(sushi_customers_path), position)

    # Should find the same references
    assert len(references) == 2
    assert all(ref.path == sushi_customers_path for ref in references)

    reference_ranges = [ref.range for ref in references]
    for expected_range in ranges:
        assert any(
            ref_range.start.line == expected_range.start.line
            and ref_range.start.character == expected_range.start.character
            for ref_range in reference_ranges
        ), (
            f"Expected to find reference at line {expected_range.start.line}, char {expected_range.start.character}"
        )


def test_cte_no_references_on_non_cte():
    # Test that clicking on non-CTE elements returns nothing, once this is supported adapt this test accordingly
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Click on a regular table reference
    ranges = find_ranges_from_regex(read_file, r"sushi\.orders")
    assert len(ranges) >= 1

    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    references = get_cte_references(lsp_context, URI.from_path(sushi_customers_path), position)

    # Should find no references since this is not a CTE
    assert len(references) == 0
