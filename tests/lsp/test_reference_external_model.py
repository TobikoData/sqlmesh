from lsprotocol.types import Position
from sqlmesh.core.context import Context
from sqlmesh.core.linter.helpers import read_range_from_file
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.helpers import to_sqlmesh_range
from sqlmesh.lsp.reference import get_references, LSPExternalModelReference
from sqlmesh.lsp.uri import URI


def test_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    customers = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    # Position of reference in file sushi.customers for sushi.raw_demographics
    position = Position(line=42, character=20)
    references = get_references(lsp_context, URI.from_path(customers), position)

    assert len(references) == 1
    reference = references[0]
    assert isinstance(reference, LSPExternalModelReference)
    assert reference.uri.endswith("external_models.yaml")

    source_range = read_range_from_file(customers, to_sqlmesh_range(reference.range))
    assert source_range == "raw.demographics"

    if reference.target_range is None:
        raise AssertionError("Reference target range should not be None")
    target_range = read_range_from_file(
        URI(reference.uri).to_path(), to_sqlmesh_range(reference.target_range)
    )
    assert target_range == "raw.demographics"
