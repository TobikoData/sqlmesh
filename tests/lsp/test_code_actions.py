import typing as t
import os
from lsprotocol import types
from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.uri import URI


def test_code_actions_with_linting(copy_to_temp_path: t.Callable):
    """Test that code actions are generated for linting violations."""

    # Copy sushi example to a temporary directory
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    #  Override the config and turn the linter on
    config_path = sushi_path / "config.py"
    with config_path.open("r") as f:
        lines = f.readlines()
    lines = [
        line.replace("enabled=False,", "enabled=True,") if "enabled=False," in line else line
        for line in lines
    ]
    with config_path.open("w") as f:
        f.writelines(lines)

    # Override the latest_order.sql file to introduce a linter violation
    model_content = """MODEL (
  name sushi.latest_order,
  kind CUSTOM (
    materialization 'custom_full_with_custom_kind',
    materialization_properties (
      custom_property = 'sushi!!!'
    )
  ),
  cron '@daily'
);

SELECT *
FROM sushi.orders
ORDER BY event_date DESC LIMIT 1
"""
    latest_order_path = sushi_path / "models" / "latest_order.sql"
    with latest_order_path.open("w") as f:
        f.write(model_content)

    # Create context with the mocked config
    context = Context(paths=[str(sushi_path)])

    # Create LSP context
    lsp_context = LSPContext(context)

    # Get diagnostics (linting violations)
    violations = lsp_context.lint_model(URI.from_path(sushi_path / "models" / "latest_order.sql"))

    uri = URI.from_path(sushi_path / "models" / "latest_order.sql")

    # First, convert violations to LSP diagnostics
    diagnostics = []
    for violation in violations:
        if violation.violation_range:
            diagnostic = types.Diagnostic(
                range=types.Range(
                    start=types.Position(
                        line=violation.violation_range.start.line,
                        character=violation.violation_range.start.character,
                    ),
                    end=types.Position(
                        line=violation.violation_range.end.line,
                        character=violation.violation_range.end.character,
                    ),
                ),
                message=violation.violation_msg,
                severity=types.DiagnosticSeverity.Warning,
            )
            diagnostics.append(diagnostic)

        # Create code action params with diagnostics
        params = types.CodeActionParams(
            text_document=types.TextDocumentIdentifier(uri=uri.value),
            range=types.Range(
                start=types.Position(line=0, character=0),
                end=types.Position(line=100, character=0),
            ),
            context=types.CodeActionContext(diagnostics=diagnostics),
        )

    # Get code actions
    code_actions = lsp_context.get_code_actions(
        URI.from_path(sushi_path / "models" / "latest_order.sql"), params
    )

    # Verify we have code actions
    assert code_actions is not None
    assert len(code_actions) > 0

    # Verify the code action properties
    first_action = code_actions[0]
    if not isinstance(first_action, types.CodeAction):
        raise AssertionError("First action is not a CodeAction instance")
    assert first_action.kind == types.CodeActionKind.QuickFix
    assert first_action.edit is not None
    assert first_action.edit.changes is not None
    assert (
        URI.from_path(sushi_path / "models" / "latest_order.sql").value in first_action.edit.changes
    )

    # The fix should replace SELECT * with specific columns
    text_edits = first_action.edit.changes[
        URI.from_path(sushi_path / "models" / "latest_order.sql").value
    ]
    assert len(text_edits) > 0


def test_code_actions_create_file(copy_to_temp_path: t.Callable) -> None:
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    # Remove external models file and enable linter
    os.remove(sushi_path / "external_models.yaml")
    config_path = sushi_path / "config.py"
    with config_path.open("r") as f:
        content = f.read()

    before = """    linter=LinterConfig(
        enabled=False,
        rules=[
            "ambiguousorinvalidcolumn",
            "invalidselectstarexpansion",
            "noselectstar",
            "nomissingaudits",
            "nomissingowner",
            "nomissingexternalmodels",
        ],
    ),"""
    after = """linter=LinterConfig(enabled=True, rules=["nomissingexternalmodels"]),"""
    content = content.replace(before, after)
    with config_path.open("w") as f:
        f.write(content)

    context = Context(paths=[str(sushi_path)])
    lsp_context = LSPContext(context)

    uri = URI.from_path(sushi_path / "models" / "customers.sql")
    violations = lsp_context.lint_model(uri)

    diagnostics = []
    for violation in violations:
        if violation.violation_range:
            diagnostics.append(
                types.Diagnostic(
                    range=types.Range(
                        start=types.Position(
                            line=violation.violation_range.start.line,
                            character=violation.violation_range.start.character,
                        ),
                        end=types.Position(
                            line=violation.violation_range.end.line,
                            character=violation.violation_range.end.character,
                        ),
                    ),
                    message=violation.violation_msg,
                    severity=types.DiagnosticSeverity.Warning,
                )
            )

    params = types.CodeActionParams(
        text_document=types.TextDocumentIdentifier(uri=uri.value),
        range=types.Range(
            start=types.Position(line=0, character=0), end=types.Position(line=1, character=0)
        ),
        context=types.CodeActionContext(diagnostics=diagnostics),
    )

    actions = lsp_context.get_code_actions(uri, params)
    assert actions is not None and len(actions) > 0
    action = next(a for a in actions if isinstance(a, types.CodeAction))
    assert action.edit is not None
    assert action.edit.document_changes is not None
    create_file = [c for c in action.edit.document_changes if isinstance(c, types.CreateFile)]
    assert create_file, "Expected a CreateFile operation"
    assert create_file[0].uri == URI.from_path(sushi_path / "external_models.yaml").value
