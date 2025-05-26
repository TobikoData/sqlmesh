import typing as t
from pathlib import Path
import pytest
import subprocess
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.utils import yaml
import shutil
import site

pytestmark = pytest.mark.slow


class InvokeCliType(t.Protocol):
    def __call__(
        self, sqlmesh_args: t.List[str], **kwargs: t.Any
    ) -> subprocess.CompletedProcess: ...


@pytest.fixture
def invoke_cli(tmp_path: Path) -> InvokeCliType:
    # Fetch the full path to the SQLMesh binary so that when we use `cwd` to run in the context of a test dir, the correct SQLMesh binary is executed
    # this will be the current project because `make install-dev` installs an editable version of SQLMesh into the current python environment
    sqlmesh_bin = subprocess.run(
        ["which", "sqlmesh"], capture_output=True, text=True
    ).stdout.strip()

    def _invoke(sqlmesh_args: t.List[str], **kwargs: t.Any) -> subprocess.CompletedProcess:
        return subprocess.run(
            args=[sqlmesh_bin] + sqlmesh_args,
            # set the working directory to the isolated temp dir for this test
            cwd=tmp_path,
            # return text instead of binary from the output streams
            text=True,
            # combine stdout/stderr into a single stream
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **kwargs,
        )

    return _invoke


def test_load_snapshots_that_reference_nonexistent_python_libraries(
    invoke_cli: InvokeCliType, tmp_path: Path
) -> None:
    init_example_project(tmp_path, dialect="duckdb")
    config_path = tmp_path / "config.yaml"

    # we need state to persist between invocations
    config_dict = yaml.load(config_path)
    config_dict["gateways"]["duckdb"]["state_connection"] = {
        "type": "duckdb",
        "database": str(tmp_path / "state.db"),
    }
    config_path.write_text(yaml.dump(config_dict))

    # simulate a 3rd party library that provides a macro
    site_packages = site.getsitepackages()[0]
    sqlmesh_test_macros_package_path = Path(site_packages) / "sqlmesh_test_macros"
    sqlmesh_test_macros_package_path.mkdir()
    (sqlmesh_test_macros_package_path / "macros.py").write_text("""
from sqlmesh import macro

@macro()
def do_something(evaluator):
    return "'value from site-packages'"
""")

    # reference the macro from site-packages
    (tmp_path / "macros" / "__init__.py").write_text("""
from sqlmesh_test_macros.macros import do_something
""")

    (tmp_path / "models" / "example.sql").write_text("""
MODEL (
    name example.test_model,
    kind FULL
);

select @do_something() as a
""")

    result = invoke_cli(["plan", "--no-prompts", "--auto-apply", "--skip-tests"])

    assert result.returncode == 0
    assert "Physical layer updated" in result.stdout
    assert "Virtual layer updated" in result.stdout

    # render the query to ensure our macro is being invoked
    result = invoke_cli(["render", "example.test_model"])
    assert result.returncode == 0
    assert """SELECT 'value from site-packages' AS "a\"""" in " ".join(result.stdout.split())

    # clear cache to ensure we are forced to reload everything
    assert invoke_cli(["clean"]).returncode == 0

    # deleting this removes the 'do_something()' macro used by the version of the snapshot stored in state
    # when loading the old snapshot from state in the local python env, this will create an ImportError
    shutil.rmtree(sqlmesh_test_macros_package_path)

    # Move the macro inline so its no longer being loaded from a library but still exists with the same signature
    (tmp_path / "macros" / "__init__.py").write_text("""
from sqlmesh import macro

@macro()
def do_something(evaluator):
    return "'some value not from site-packages'"
""")

    # this should produce an error but not a fatal one. there will be an error rendering the optimized query of the old snapshot, which should be logged
    result = invoke_cli(
        [
            "plan",
            "--no-prompts",
            "--auto-apply",
            "--skip-tests",
        ]
    )
    assert result.returncode == 0
    assert "Physical layer updated" in result.stdout
    assert "Virtual layer updated" in result.stdout

    log_file = sorted(list((tmp_path / "logs").iterdir()))[-1]
    log_file_contents = log_file.read_text()
    assert "ModuleNotFoundError: No module named 'sqlmesh_test_macros'" in log_file_contents
    assert (
        "ERROR - Failed to cache optimized query for model 'example.test_model'"
        in log_file_contents
    )
    assert (
        'ERROR - Failed to cache snapshot SnapshotId<"db"."example"."test_model"'
        in log_file_contents
    )
