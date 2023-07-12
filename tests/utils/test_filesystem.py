import pathlib


def create_temp_file(tmp_path: pathlib.Path, filepath: pathlib.Path, contents: str) -> pathlib.Path:
    target_filepath = tmp_path / filepath
    target_filepath.parent.mkdir(parents=True, exist_ok=True)
    target_filepath.write_text(contents)
    assert target_filepath.read_text() == contents
    return target_filepath
