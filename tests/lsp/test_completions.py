import pytest
from sqlglot import Tokenizer
from sqlmesh.core.context import Context
from sqlmesh.lsp.completions import get_keywords_from_tokenizer, get_sql_completions
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.uri import URI


TOKENIZER_KEYWORDS = set(Tokenizer.KEYWORDS.keys())


@pytest.mark.fast
def test_get_keywords_from_tokenizer():
    assert len(get_keywords_from_tokenizer()) >= len(TOKENIZER_KEYWORDS)


@pytest.mark.fast
def test_get_sql_completions_no_context():
    completions = get_sql_completions(None, None)
    assert len(completions.keywords) >= len(TOKENIZER_KEYWORDS)
    assert len(completions.models) == 0


@pytest.mark.fast
def test_get_sql_completions_with_context_no_file_uri():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    completions = lsp_context.get_autocomplete(None)
    assert len(completions.keywords) > len(TOKENIZER_KEYWORDS)
    assert "sushi.active_customers" in completions.models
    assert "sushi.customers" in completions.models


@pytest.mark.fast
def test_get_sql_completions_with_context_and_file_uri():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    file_uri = next(key for key in lsp_context.map.keys() if key.name == "active_customers.sql")
    completions = lsp_context.get_autocomplete(URI.from_path(file_uri))
    assert len(completions.keywords) > len(TOKENIZER_KEYWORDS)
    assert "sushi.active_customers" not in completions.models
