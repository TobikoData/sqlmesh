import os

from sqlmesh import Context


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
    assert (
        "Model 'sushi.customers' depends on unregistered external models: "
        in lints[0].violation_msg
    )
