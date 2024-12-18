import json
import os
import pytest
from pathlib import Path

from sqlmesh.core.context import Context
from sqlmesh.core import config as sqlmesh_config

def test_cube_generate_json():
    # Get the path to the examples/ecommerce directory
    examples_dir = Path(__file__).parent.parent.parent.parent / "examples" / "ecommerce"
    
    # Create a Context with the ecommerce project
    context = Context(paths=str(examples_dir))
    
    # Generate cube data for gold tables
    from io import StringIO
    import sys
    
    # Capture stdout
    stdout = StringIO()
    sys.stdout = stdout
    
    # Generate cube data
    context.cube_generate(
        select_models=["gold.*"]
    )
    
    # Restore stdout
    sys.stdout = sys.__stdout__
    
    # Get captured output and parse as JSON
    cube_data = json.loads(stdout.getvalue())
    stdout.close()

    # Load expected output
    expected_path = examples_dir / "test_output.json"
    with open(expected_path) as f:
        expected_data = json.load(f)
    
    # Sort both outputs by model name for consistent comparison
    cube_data = sorted(cube_data, key=lambda x: x["model"])
    expected_data = sorted(expected_data, key=lambda x: x["model"])

    # Compare actual and expected outputs
    assert cube_data == expected_data, "Generated cube data does not match expected output"
