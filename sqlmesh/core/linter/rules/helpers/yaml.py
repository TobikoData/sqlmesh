from pathlib import Path

from ruamel.yaml import YAML

from sqlmesh.core.linter.rule import Range, Position
import typing as t


def _get_yaml_model_range(path: Path, model_name: str) -> t.Optional[Range]:
    """
    Find the range of a specific model block in a YAML file.

    Args:
        yaml_path: Path to the YAML file
        model_name: Name of the model to find

    Returns:
        The Range of the model block in the YAML file, or None if not found
    """
    yaml = YAML()
    with path.open("r", encoding="utf-8") as f:
        data = yaml.load(f)

    if not isinstance(data, list):
        return None

    for item in data:
        if isinstance(item, dict) and item.get("name") == model_name:
            # Get size of block by taking the earliest line/col in the items block and the last line/col of the block
            position_data = item.lc.data["name"]  # type: ignore
            start = Position(line=position_data[2], character=position_data[3])
            end = Position(line=position_data[2], character=position_data[3] + len(item["name"]))
            return Range(start=start, end=end)
    return None
