from sqlglot import Tokenizer
from sqlmesh.core.context import Context
from sqlmesh.lsp.completions import (
    get_keywords_from_tokenizer,
    get_sql_completions,
    extract_keywords_from_content,
)
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.uri import URI


TOKENIZER_KEYWORDS = set(Tokenizer.KEYWORDS.keys())


def test_get_keywords_from_tokenizer():
    assert len(get_keywords_from_tokenizer()) >= len(TOKENIZER_KEYWORDS)


def test_get_sql_completions_no_context():
    completions = get_sql_completions(None, None)
    assert len(completions.keywords) >= len(TOKENIZER_KEYWORDS)
    assert len(completions.models) == 0


def test_get_macros():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    file_path = next(key for key in lsp_context.map.keys() if key.name == "active_customers.sql")
    with open(file_path, "r", encoding="utf-8") as f:
        file_content = f.read()

    file_uri = URI.from_path(file_path)
    completions = LSPContext.get_completions(lsp_context, file_uri, file_content)

    each_macro = next((m for m in completions.macros if m.name == "each"))
    assert each_macro.name == "each"
    assert each_macro.description
    add_one_macro = next((m for m in completions.macros if m.name == "add_one"))
    assert add_one_macro.name == "add_one"
    assert add_one_macro.description


def test_model_completions_include_descriptions():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    completions = LSPContext.get_completions(lsp_context, None)

    model_entry = next(
        (m for m in completions.model_completions if m.name == "sushi.customers"),
        None,
    )
    assert model_entry is not None
    assert model_entry.description


def test_get_sql_completions_with_context_no_file_uri():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    completions = LSPContext.get_completions(lsp_context, None)
    assert len(completions.keywords) >= len(TOKENIZER_KEYWORDS)
    assert "sushi.active_customers" in completions.models
    assert "sushi.customers" in completions.models


def test_get_sql_completions_with_context_and_file_uri():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    file_uri = next(key for key in lsp_context.map.keys() if key.name == "active_customers.sql")
    completions = LSPContext.get_completions(lsp_context, URI.from_path(file_uri))
    assert len(completions.keywords) > len(TOKENIZER_KEYWORDS)
    assert "sushi.active_customers" not in completions.models


def test_extract_keywords_from_content():
    # Test extracting keywords from SQL content
    content = """
    SELECT customer_id, order_date, total_amount
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE order_date > '2024-01-01'
    """

    keywords = extract_keywords_from_content(content)

    # Check that identifiers are extracted
    assert "customer_id" in keywords
    assert "order_date" in keywords
    assert "total_amount" in keywords
    assert "orders" in keywords
    assert "customers" in keywords
    assert "o" in keywords  # alias
    assert "c" in keywords  # alias
    assert "id" in keywords

    # Check that SQL keywords are NOT included
    assert "SELECT" not in keywords
    assert "FROM" not in keywords
    assert "JOIN" not in keywords
    assert "WHERE" not in keywords
    assert "ON" not in keywords


def test_get_sql_completions_with_file_content():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # SQL content with custom identifiers
    content = """
    SELECT my_custom_column, another_identifier
    FROM my_custom_table mct
    JOIN some_other_table sot ON mct.id = sot.table_id
    WHERE my_custom_column > 100
    """

    file_uri = next(key for key in lsp_context.map.keys() if key.name == "active_customers.sql")
    completions = LSPContext.get_completions(lsp_context, URI.from_path(file_uri), content)

    # Check that SQL keywords are included
    assert any(k in ["SELECT", "FROM", "WHERE", "JOIN"] for k in completions.keywords)

    # Check that file-specific identifiers are included at the end
    keywords_list = completions.keywords
    assert "my_custom_column" in keywords_list
    assert "another_identifier" in keywords_list
    assert "my_custom_table" in keywords_list
    assert "some_other_table" in keywords_list
    assert "mct" in keywords_list  # alias
    assert "sot" in keywords_list  # alias
    assert "table_id" in keywords_list

    # Check that file keywords come after SQL keywords
    # SQL keywords should appear first in the list
    sql_keyword_indices = [
        i for i, k in enumerate(keywords_list) if k in ["SELECT", "FROM", "WHERE", "JOIN"]
    ]
    file_keyword_indices = [
        i for i, k in enumerate(keywords_list) if k in ["my_custom_column", "my_custom_table"]
    ]

    if sql_keyword_indices and file_keyword_indices:
        assert max(sql_keyword_indices) < min(file_keyword_indices), (
            "SQL keywords should come before file keywords"
        )


def test_get_sql_completions_with_partial_cte_query():
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Partial SQL query with CTEs
    content = """
    WITH _latest_complete_month AS (
        SELECT MAX(date_trunc('month', order_date)) as month
        FROM orders
    ),
    _filtered AS (
        SELECT * FROM
    """

    file_uri = next(key for key in lsp_context.map.keys() if key.name == "active_customers.sql")
    completions = LSPContext.get_completions(lsp_context, URI.from_path(file_uri), content)

    # Check that CTE names are included in the keywords
    keywords_list = completions.keywords
    assert "_latest_complete_month" in keywords_list
    assert "_filtered" in keywords_list

    # Also check other identifiers from the partial query
    assert "month" in keywords_list
    assert "order_date" in keywords_list
    assert "orders" in keywords_list


def test_extract_keywords_from_partial_query():
    # Test extracting keywords from an incomplete SQL query
    content = """
    WITH cte1 AS (
        SELECT col1, col2 FROM table1
    ),
    cte2 AS (
        SELECT * FROM cte1 WHERE
    """

    keywords = extract_keywords_from_content(content)

    # Check that CTEs are extracted
    assert "cte1" in keywords
    assert "cte2" in keywords

    # Check that columns and tables are extracted
    assert "col1" in keywords
    assert "col2" in keywords
    assert "table1" in keywords
