import os

from sqlmesh import Context
from sqlmesh.core.linter.rule import Position, Range


def test_no_missing_external_models(tmp_path, copy_to_temp_path) -> None:
    """
    Tests that the linter correctly identifies unregistered external model dependencies.

    This test removes the `external_models.yaml` file from the sushi example project,
    enables the linter, and verifies that the linter raises a violation for a model
    that depends on unregistered external models.
    """
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    # Remove the external_models.yaml file
    os.remove(sushi_path / "external_models.yaml")

    # Override the config.py to turn on lint
    with open(sushi_path / "config.py", "r") as f:
        read_file = f.read()

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
    read_file = read_file.replace(before, after)
    assert after in read_file
    with open(sushi_path / "config.py", "w") as f:
        f.writelines(read_file)

    # Load the context with the temporary sushi path
    context = Context(paths=[sushi_path])

    # Lint the models
    lints = context.lint_models(raise_on_error=False)
    assert len(lints) == 1
    lint = lints[0]
    assert lint.violation_range is not None
    assert (
        lint.violation_msg
        == """Model '"memory"."sushi"."customers"' depends on unregistered external model '"memory"."raw"."demographics"'. Please register it in the external models file. This can be done by running 'sqlmesh create_external_models'."""
    )
    assert len(lint.fixes) == 1
    fix = lint.fixes[0]
    assert len(fix.edits) == 0
    assert len(fix.create_files) == 1
    create = fix.create_files[0]
    assert create.path == sushi_path / "external_models.yaml"
    assert create.text == '- name: \'"memory"."raw"."demographics"\'\n'


def test_no_missing_external_models_with_existing_file_ending_in_newline(
    tmp_path, copy_to_temp_path
) -> None:
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    # Overwrite the external_models.yaml file to end with a random file and a newline
    os.remove(sushi_path / "external_models.yaml")
    with open(sushi_path / "external_models.yaml", "w") as f:
        f.write("- name: memory.raw.test\n")

    # Override the config.py to turn on lint
    with open(sushi_path / "config.py", "r") as f:
        read_file = f.read()

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
    read_file = read_file.replace(before, after)
    assert after in read_file
    with open(sushi_path / "config.py", "w") as f:
        f.writelines(read_file)

    # Load the context with the temporary sushi path
    context = Context(paths=[sushi_path])

    # Lint the models
    lints = context.lint_models(raise_on_error=False)
    assert len(lints) == 1
    lint = lints[0]
    assert lint.violation_range is not None
    assert (
        lint.violation_msg
        == """Model '"memory"."sushi"."customers"' depends on unregistered external model '"memory"."raw"."demographics"'. Please register it in the external models file. This can be done by running 'sqlmesh create_external_models'."""
    )
    assert len(lint.fixes) == 1
    fix = lint.fixes[0]
    assert len(fix.edits) == 1
    edit = fix.edits[0]
    assert edit.new_text == """- name: '"memory"."raw"."demographics"'\n"""
    assert edit.range == Range(
        start=Position(line=1, character=0),
        end=Position(line=1, character=0),
    )
    fix_path = sushi_path / "external_models.yaml"
    assert edit.path == fix_path


def test_no_missing_external_models_with_existing_file_not_ending_in_newline(
    tmp_path, copy_to_temp_path
) -> None:
    sushi_paths = copy_to_temp_path("examples/sushi")
    sushi_path = sushi_paths[0]

    # Overwrite the external_models.yaml file to end with a random file and a newline
    os.remove(sushi_path / "external_models.yaml")
    with open(sushi_path / "external_models.yaml", "w") as f:
        f.write("- name: memory.raw.test")

    # Override the config.py to turn on lint
    with open(sushi_path / "config.py", "r") as f:
        read_file = f.read()

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
    read_file = read_file.replace(before, after)
    assert after in read_file
    with open(sushi_path / "config.py", "w") as f:
        f.writelines(read_file)

    # Load the context with the temporary sushi path
    context = Context(paths=[sushi_path])

    # Lint the models
    lints = context.lint_models(raise_on_error=False)
    assert len(lints) == 1
    lint = lints[0]
    assert lint.violation_range is not None
    assert (
        lint.violation_msg
        == """Model '"memory"."sushi"."customers"' depends on unregistered external model '"memory"."raw"."demographics"'. Please register it in the external models file. This can be done by running 'sqlmesh create_external_models'."""
    )
    assert len(lint.fixes) == 1
    fix = lint.fixes[0]
    assert len(fix.edits) == 1
    edit = fix.edits[0]
    assert edit.new_text == """\n- name: '"memory"."raw"."demographics"'\n"""
    assert edit.range == Range(
        start=Position(line=0, character=23),
        end=Position(line=0, character=23),
    )
    fix_path = sushi_path / "external_models.yaml"
    assert edit.path == fix_path
