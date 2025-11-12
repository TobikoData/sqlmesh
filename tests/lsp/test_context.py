from pathlib import Path

from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.uri import URI


def test_lsp_context():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    assert lsp_context is not None
    assert lsp_context.context is not None
    assert lsp_context.map is not None

    # find one model in the map
    active_customers_key = next(
        key for key in lsp_context.map.keys() if key.name == "active_customers.sql"
    )

    # Check that the value is a ModelInfo with the expected model name
    assert isinstance(lsp_context.map[active_customers_key], ModelTarget)
    assert "sushi.active_customers" in lsp_context.map[active_customers_key].names


def test_lsp_context_list_workspace_tests():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # List workspace tests
    tests = lsp_context.list_workspace_tests()

    # Check that the tests are returned correctly
    assert len(tests) == 3
    assert any(test.name == "test_order_items" for test in tests)


def test_lsp_context_get_document_tests():
    test_path = Path.cwd() / "examples/sushi/tests/test_order_items.yaml"
    uri = URI.from_path(test_path)

    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)
    tests = lsp_context.get_document_tests(uri)

    assert len(tests) == 1
    assert tests[0].uri == uri.value
    assert tests[0].name == "test_order_items"


def test_lsp_context_run_test():
    test_path = Path.cwd() / "examples/sushi/tests/test_order_items.yaml"
    uri = URI.from_path(test_path)

    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Run the test
    result = lsp_context.run_test(uri, "test_order_items")

    # Check that the result is not None and has the expected properties
    assert result is not None
    assert result.success is True
