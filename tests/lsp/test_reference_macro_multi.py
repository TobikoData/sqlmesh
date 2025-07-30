from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import MacroReference, get_macro_definitions_for_a_path
from sqlmesh.lsp.uri import URI


def test_macro_references_multirepo() -> None:
    context = Context(paths=["examples/multi/repo_1", "examples/multi/repo_2"], gateway="memory")
    lsp_context = LSPContext(context)

    d_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "silver.d" in info.names
    )

    d = URI.from_path(d_path)
    macro_references = get_macro_definitions_for_a_path(lsp_context, d)

    assert len(macro_references) == 2
    for ref in macro_references:
        assert isinstance(ref, MacroReference)
        assert str(URI.from_path(ref.path).value).endswith("multi/repo_2/macros/__init__.py")
        assert ref.target_range is not None
