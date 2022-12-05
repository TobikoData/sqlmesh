import pathlib

import py.error


def create_temp_file(tmpdir, filepath: pathlib.Path, contents: str) -> pathlib.Path:
    target_dir = tmpdir
    for directory in list(filepath.parts)[:-1]:
        try:
            target_dir = target_dir.mkdir(directory)  # type: ignore
        except py.error.EEXIST:
            target_dir = target_dir.join(directory)  # type: ignore
    target_filepath = target_dir.join(filepath.name)  # type: ignore
    target_filepath.write(contents)
    assert target_filepath.read() == contents
    return target_filepath
