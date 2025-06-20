from lsprotocol.types import Position, DocumentHighlightKind

from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.rename import get_document_highlights
from sqlmesh.lsp.uri import URI
from tests.lsp.test_reference_cte import find_ranges_from_regex


def test_get_document_highlights_cte():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Use the existing customers.sql model which has CTEs
    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    test_uri = URI.from_path(sushi_customers_path)

    # Find the ranges for "current_marketing" CTE (not outer one)
    ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(ranges) >= 2  # Should have definition + usage

    # Test highlighting CTE definition - position on "current_marketing" definition
    position = Position(line=ranges[0].start.line, character=ranges[0].start.character + 4)
    highlights = get_document_highlights(lsp_context, test_uri, position)

    assert highlights is not None
    assert len(highlights) >= 2  # Definition + at least 1 usage

    # Check that we have both definition (Write) and usage (Read) highlights
    highlight_kinds = [h.kind for h in highlights]
    assert DocumentHighlightKind.Write in highlight_kinds  # CTE definition
    assert DocumentHighlightKind.Read in highlight_kinds  # CTE usage

    # Test highlighting CTE usage - position on "current_marketing" usage
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    highlights = get_document_highlights(lsp_context, test_uri, position)

    assert highlights is not None
    assert len(highlights) >= 2  # Should find the same references


def test_get_document_highlights_no_symbol():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Use the existing customers.sql model
    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    test_uri = URI.from_path(sushi_customers_path)

    # Test position not on any CTE symbol - just on a random keyword
    position = Position(line=5, character=5)
    highlights = get_document_highlights(lsp_context, test_uri, position)

    assert highlights is None


def test_get_document_highlights_multiple_ctes():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Use the existing customers.sql model which has both outer and inner CTEs
    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    test_uri = URI.from_path(sushi_customers_path)

    # Test the outer CTE - "current_marketing_outer"
    outer_ranges = find_ranges_from_regex(read_file, r"current_marketing_outer")
    assert len(outer_ranges) >= 2  # Should have definition + usage

    # Test highlighting outer CTE - should only highlight that CTE
    position = Position(
        line=outer_ranges[0].start.line, character=outer_ranges[0].start.character + 4
    )
    highlights = get_document_highlights(lsp_context, test_uri, position)

    assert highlights is not None
    assert len(highlights) == len(outer_ranges)  # Should match all occurrences of outer CTE

    # Test the inner CTE - "current_marketing" (not outer)
    inner_ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(inner_ranges) >= 2  # Should have definition + usage

    # Test highlighting inner CTE - should only highlight that CTE, not the outer one
    position = Position(
        line=inner_ranges[0].start.line, character=inner_ranges[0].start.character + 4
    )
    highlights = get_document_highlights(lsp_context, test_uri, position)

    # This should return the column usages as well
    assert highlights is not None
    assert len(highlights) == 4
