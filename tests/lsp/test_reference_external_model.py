import os
from pathlib import Path

from sqlmesh import Config
from sqlmesh.core.context import Context
from sqlmesh.core.linter.helpers import read_range_from_file
from sqlmesh.core.linter.rule import Position
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.reference import get_references
from sqlmesh.lsp.uri import URI
from sqlmesh.utils.lineage import ExternalModelReference
from tests.utils.test_filesystem import create_temp_file
import typing as t


def test_reference() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    customers = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customers" in info.names
    )

    # Position of reference in file sushi.customers for sushi.raw_demographics
    position = Position(line=42, character=20)
    references = get_references(lsp_context, URI.from_path(customers), position)

    assert len(references) == 1
    reference = references[0]
    assert isinstance(reference, ExternalModelReference)
    path = reference.path
    assert path is not None
    assert str(path).endswith("external_models.yaml")

    source_range = read_range_from_file(customers, reference.range)
    assert source_range == "raw.demographics"

    if reference.target_range is None:
        raise AssertionError("Reference target range should not be None")
    path = reference.path
    assert path is not None
    target_range = read_range_from_file(path, reference.target_range)
    assert target_range == "raw.demographics"


def test_unregistered_external_model(tmp_path: Path):
    model_path = tmp_path / "models" / "foo.sql"
    contents = "MODEL (name test.foo, kind FULL); SELECT * FROM external_model"
    create_temp_file(tmp_path, model_path, contents)
    ctx = Context(paths=[tmp_path], config=Config())
    lsp_context = LSPContext(ctx)

    uri = URI.from_path(model_path)
    references = get_references(lsp_context, uri, Position(line=0, character=len(contents) - 3))

    assert len(references) == 1
    reference = references[0]
    assert isinstance(reference, ExternalModelReference)
    assert reference.path is None
    assert reference.target_range is None
    assert reference.markdown_description == "Unregistered external model"
    assert read_range_from_file(model_path, reference.range) == "external_model"


def test_unregistered_external_model_with_schema(
    copy_to_temp_path: t.Callable[[str], list[Path]],
) -> None:
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

    model = context.get_model("sushi.customers")
    if model is None:
        raise AssertionError("Model 'sushi.customers' not found in context")

    lsp_context = LSPContext(context)
    path = model._path
    assert path is not None
    uri = URI.from_path(path)
    references = get_references(lsp_context, uri, Position(line=42, character=20))

    assert len(references) == 1
    reference = references[0]
    assert isinstance(reference, ExternalModelReference)
    assert reference.path is None
    assert read_range_from_file(path, reference.range) == "raw.demographics"
