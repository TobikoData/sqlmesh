from sqlmesh import Context
from sqlmesh.core.linter.helpers import read_range_from_file
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.uri import URI


def test_diagnostic_on_sushi(tmp_path, copy_to_temp_path) -> None:
    # Copy sushi example to a temporary directory
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    # Override the active_customers.sql file to introduce a linter violation
    active_customers_path = sushi_path / "models" / "active_customers.sql"
    # Replace SELECT customer_id, zip with SELECT * to trigger a linter violation
    with active_customers_path.open("r") as f:
        lines = f.readlines()
    lines = [
        line.replace("SELECT customer_id, zip", "SELECT *")
        if "SELECT customer_id, zip" in line
        else line
        for line in lines
    ]
    with active_customers_path.open("w") as f:
        f.writelines(lines)

    # Override the config and turn the linter on
    config_path = sushi_path / "config.py"
    with config_path.open("r") as f:
        lines = f.readlines()
    lines = [
        line.replace("enabled=False,", "enabled=True,") if "enabled=False," in line else line
        for line in lines
    ]
    with config_path.open("w") as f:
        f.writelines(lines)

    # Load the context with the temporary sushi path
    context = Context(paths=[str(sushi_path)])
    lsp_context = LSPContext(context)

    # Diagnostics should be available
    active_customers_uri = URI.from_path(active_customers_path)
    lsp_diagnostics = lsp_context.lint_model(active_customers_uri)

    assert len(lsp_diagnostics) > 0

    # Get the no select star diagnostic
    select_star_diagnostic = [diag for diag in lsp_diagnostics if diag.rule.name == "noselectstar"]
    assert len(select_star_diagnostic) == 1
    diagnostic = select_star_diagnostic[0]

    assert diagnostic.violation_range

    contents = read_range_from_file(active_customers_path, diagnostic.violation_range)
    assert contents == "*"
