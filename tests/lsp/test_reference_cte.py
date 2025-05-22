import re
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import get_references
from sqlmesh.lsp.uri import URI
from lsprotocol.types import Range, Position
import typing as t


def test_cte_parsing():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    sushi_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    with open(sushi_customers_path, "r", encoding="utf-8") as file:
        read_file = file.readlines()

    # Find position of the cte reference
    ranges = find_ranges_from_regex(read_file, r"current_marketing(?!_outer)")
    assert len(ranges) == 2
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    references = get_references(lsp_context, URI.from_path(sushi_customers_path), position)
    assert len(references) == 1
    assert references[0].uri == URI.from_path(sushi_customers_path).value
    assert references[0].markdown_description is None
    assert (
        references[0].range.start.line == ranges[1].start.line
    )  # The reference location (where we clicked)
    assert (
        references[0].target_range.start.line == ranges[0].start.line
    )  # The CTE definition location

    # Find the position of the current_marketing_outer reference
    ranges = find_ranges_from_regex(read_file, r"current_marketing_outer")
    assert len(ranges) == 2
    position = Position(line=ranges[1].start.line, character=ranges[1].start.character + 4)
    references = get_references(lsp_context, URI.from_path(sushi_customers_path), position)
    assert len(references) == 1
    assert references[0].uri == URI.from_path(sushi_customers_path).value
    assert references[0].markdown_description is None
    assert (
        references[0].range.start.line == ranges[1].start.line
    )  # The reference location (where we clicked)
    assert (
        references[0].target_range.start.line == ranges[0].start.line
    )  # The CTE definition location


def find_ranges_from_regex(read_file: t.List[str], regex: str) -> t.List[Range]:
    """Find all ranges in the read file that match the regex."""
    return [
        Range(
            start=Position(line=line_number, character=match.start()),
            end=Position(line=line_number, character=match.end()),
        )
        for line_number, line in enumerate(read_file)
        for match in [m for m in [re.search(regex, line)] if m]
    ]
