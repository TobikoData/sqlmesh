from lsprotocol.types import Position
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import (
    get_macro_find_all_references,
    get_macro_definitions_for_a_path,
)
from sqlmesh.lsp.uri import URI
from sqlmesh.core.linter.helpers import (
    read_range_from_file,
    Range as SQLMeshRange,
    Position as SQLMeshPosition,
)


def test_find_all_references_for_macro_add_one():
    """Test finding all references to the @ADD_ONE macro."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the top_waiters model that uses @ADD_ONE macro
    top_waiters_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.top_waiters" in info.names
    )

    top_waiters_uri = URI.from_path(top_waiters_path)
    macro_references = get_macro_definitions_for_a_path(lsp_context, top_waiters_uri)

    # Find the @ADD_ONE reference
    add_one_ref = next((ref for ref in macro_references if ref.range.start.line == 12), None)
    assert add_one_ref is not None, "Should find @ADD_ONE reference in top_waiters"

    # Click on the @ADD_ONE macro at line 13, character 5 (the @ symbol)
    position = Position(line=12, character=5)

    all_references = get_macro_find_all_references(lsp_context, top_waiters_uri, position)

    # Should find at least 2 references: the definition and the usage in top_waiters
    assert len(all_references) >= 2, f"Expected at least 2 references, found {len(all_references)}"

    # Verify the macro definition is included
    definition_refs = [ref for ref in all_references if "utils.py" in ref.uri]
    assert len(definition_refs) >= 1, "Should include the macro definition in utils.py"

    # Verify the usage in top_waiters is included
    usage_refs = [ref for ref in all_references if "top_waiters" in ref.uri]
    assert len(usage_refs) >= 1, "Should include the usage in top_waiters.sql"

    expected_files = {
        "utils.py": {"pattern": r"def add_one", "expected_content": "def add_one"},
        "customers.sql": {"pattern": r"@ADD_ONE\s*\(", "expected_content": "ADD_ONE"},
        "top_waiters.sql": {"pattern": r"@ADD_ONE\s*\(", "expected_content": "ADD_ONE"},
    }

    for expected_file, expectations in expected_files.items():
        file_refs = [ref for ref in all_references if expected_file in ref.uri]
        assert len(file_refs) >= 1, f"Should find at least one reference in {expected_file}"

        file_ref = file_refs[0]
        file_path = URI(file_ref.uri).to_path()

        sqlmesh_range = SQLMeshRange(
            start=SQLMeshPosition(
                line=file_ref.range.start.line, character=file_ref.range.start.character
            ),
            end=SQLMeshPosition(
                line=file_ref.range.end.line, character=file_ref.range.end.character
            ),
        )

        # Read the content at the reference location
        content = read_range_from_file(file_path, sqlmesh_range)
        assert content.startswith(expectations["expected_content"]), (
            f"Expected content to start with '{expectations['expected_content']}', got: {content}"
        )


def test_find_all_references_for_macro_multiply():
    """Test finding all references to the @MULTIPLY macro."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the top_waiters model that uses @MULTIPLY macro
    top_waiters_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.top_waiters" in info.names
    )

    top_waiters_uri = URI.from_path(top_waiters_path)
    macro_references = get_macro_definitions_for_a_path(lsp_context, top_waiters_uri)

    # Find the @MULTIPLY reference
    multiply_ref = next((ref for ref in macro_references if ref.range.start.line == 13), None)
    assert multiply_ref is not None, "Should find @MULTIPLY reference in top_waiters"

    # Click on the @MULTIPLY macro at line 14, character 5 (the @ symbol)
    position = Position(line=13, character=5)
    all_references = get_macro_find_all_references(lsp_context, top_waiters_uri, position)

    # Should find at least 2 references: the definition and the usage
    assert len(all_references) >= 2, f"Expected at least 2 references, found {len(all_references)}"

    # Verify both definition and usage are included
    assert any("utils.py" in ref.uri for ref in all_references), "Should include macro definition"
    assert any("top_waiters" in ref.uri for ref in all_references), "Should include usage"


def test_find_all_references_for_sql_literal_macro():
    """Test finding references to @SQL_LITERAL macro ."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find the top_waiters model that uses @SQL_LITERAL macro
    top_waiters_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.top_waiters" in info.names
    )

    top_waiters_uri = URI.from_path(top_waiters_path)
    macro_references = get_macro_definitions_for_a_path(lsp_context, top_waiters_uri)

    # Find the @SQL_LITERAL reference
    sql_literal_ref = next((ref for ref in macro_references if ref.range.start.line == 14), None)
    assert sql_literal_ref is not None, "Should find @SQL_LITERAL reference in top_waiters"

    # Click on the @SQL_LITERAL macro
    position = Position(line=14, character=5)
    all_references = get_macro_find_all_references(lsp_context, top_waiters_uri, position)

    # For user-defined macros in utils.py, should find references
    assert len(all_references) >= 2, f"Expected at least 2 references, found {len(all_references)}"


def test_find_references_from_outside_macro_position():
    """Test that clicking outside a macro doesn't return macro references."""
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    top_waiters_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.top_waiters" in info.names
    )

    top_waiters_uri = URI.from_path(top_waiters_path)

    # Click on a position that is not on a macro
    position = Position(line=0, character=0)  # First line, which is a comment
    all_references = get_macro_find_all_references(lsp_context, top_waiters_uri, position)

    # Should return empty list when not on a macro
    assert len(all_references) == 0, "Should not find macro references when not on a macro"


def test_multi_repo_macro_references():
    """Test finding macro references across multiple repositories."""
    context = Context(paths=["examples/multi/repo_1", "examples/multi/repo_2"], gateway="memory")
    lsp_context = LSPContext(context)

    # Find model 'd' which uses macros from repo_2
    d_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "silver.d" in info.names
    )

    d_uri = URI.from_path(d_path)
    macro_references = get_macro_definitions_for_a_path(lsp_context, d_uri)

    if macro_references:
        # Click on the second macro reference which appears under the same name in repo_1 ('dup')
        first_ref = macro_references[1]
        position = Position(
            line=first_ref.range.start.line, character=first_ref.range.start.character + 1
        )
        all_references = get_macro_find_all_references(lsp_context, d_uri, position)

        # Should find the definition and usage
        assert len(all_references) == 2, f"Expected 2 references, found {len(all_references)}"

        # Verify references from repo_2
        assert any("repo_2" in ref.uri for ref in all_references), "Should find macro in repo_2"

        # But not references in repo_1 since despite identical name they're different macros
        assert not any("repo_1" in ref.uri for ref in all_references), (
            "Shouldn't find macro in repo_1"
        )
