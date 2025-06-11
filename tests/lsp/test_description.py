from sqlmesh.core.context import Context
from sqlmesh.lsp.description import generate_markdown_description


def test_model_description() -> None:
    context = Context(paths=["examples/sushi"])

    model_no_description = context.get_model("sushi.order_items")
    markdown = generate_markdown_description(model_no_description)

    assert markdown == (
        "| Column | Type | Description |\n"
        "|--------|------|-------------|\n"
        "| id | INT |  |\n"
        "| order_id | INT |  |\n"
        "| item_id | INT |  |\n"
        "| quantity | INT |  |\n"
        "| event_date | DATE |  |"
    )

    model_with_description = context.get_model("sushi.customers")
    markdown = generate_markdown_description(model_with_description)

    assert markdown == (
        "Sushi customer data\n"
        "\n"
        "| Column | Type | Description |\n"
        "|--------|------|-------------|\n"
        "| customer_id | INT | customer_id uniquely identifies customers |\n"
        "| status | TEXT |  |\n"
        "| zip | TEXT |  |"
    )
