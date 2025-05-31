import typing as t
from pathlib import Path
import pytest
import subprocess
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.utils import yaml
import shutil
import site
import uuid

pytestmark = pytest.mark.slow


class InvokeCliType(t.Protocol):
    def __call__(
        self, sqlmesh_args: t.List[str], **kwargs: t.Any
    ) -> subprocess.CompletedProcess: ...


class CreateSitePackageType(t.Protocol):
    def __call__(self, name: str) -> t.Tuple[str, Path]: ...


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


@pytest.fixture
def duckdb_example_project(tmp_path: Path) -> Path:
    init_example_project(tmp_path, dialect="duckdb")
    config_path = tmp_path / "config.yaml"

    # we need state to persist between invocations
    config_dict = yaml.load(config_path)
    config_dict["gateways"]["duckdb"]["state_connection"] = {
        "type": "duckdb",
        "database": str(tmp_path / "state.db"),
    }
    config_path.write_text(yaml.dump(config_dict))

    return tmp_path


@pytest.fixture
def last_log_file_contents(tmp_path: Path) -> t.Callable[[], str]:
    def _fetch() -> str:
        log_file = sorted(list((tmp_path / "logs").iterdir()))[-1]
        return log_file.read_text()

    return _fetch


@pytest.fixture
def create_site_package() -> t.Iterator[CreateSitePackageType]:
    created_package_path = None

    def _create(name: str) -> t.Tuple[str, Path]:
        nonlocal created_package_path

        unique_id = str(uuid.uuid4())[0:8]
        package_name = f"{name}_{unique_id}"  # so that multiple tests using the same name dont clobber each other

        site_packages = site.getsitepackages()[0]
        package_path = Path(site_packages) / package_name
        package_path.mkdir()

        created_package_path = package_path

        return package_name, package_path

    yield _create

    if created_package_path:
        # cleanup
        shutil.rmtree(created_package_path, ignore_errors=True)


def test_load_snapshots_that_reference_nonexistent_python_libraries(
    invoke_cli: InvokeCliType,
    duckdb_example_project: Path,
    last_log_file_contents: t.Callable[[], str],
    create_site_package: CreateSitePackageType,
) -> None:
    """
    Scenario:
     - A model is created using a macro that is imported from an external package
     - That model is applied + snapshot committed to state
     - The external package is removed locally and the import macro import is changed to an inline definition

    Outcome:
     - `sqlmesh plan` should not exit with an ImportError when it tries to render the query of the snapshot stored in state
     - Instead, it should log a warning and proceed with applying the new model version
    """

    project_path = duckdb_example_project

    # simulate a 3rd party library that provides a macro
    package_name, package_path = create_site_package("sqlmesh_test_macros")
    (package_path / "macros.py").write_text("""
from sqlmesh import macro

@macro()
def do_something(evaluator):
    return "'value from site-packages'"
""")

    # reference the macro from site-packages
    (project_path / "macros" / "__init__.py").write_text(f"""
from {package_name}.macros import do_something
""")

    (project_path / "models" / "example.sql").write_text("""
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
    shutil.rmtree(package_path)

    # Move the macro inline so its no longer being loaded from a library but still exists with the same signature
    (project_path / "macros" / "__init__.py").write_text("""
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

    log_file_contents = last_log_file_contents()
    assert f"ModuleNotFoundError: No module named '{package_name}'" in log_file_contents
    assert (
        "ERROR - Failed to cache optimized query for model 'example.test_model'"
        in log_file_contents
    )
    assert (
        'ERROR - Failed to cache snapshot SnapshotId<"db"."example"."test_model"'
        in log_file_contents
    )


def test_model_selector_snapshot_references_nonexistent_python_libraries(
    invoke_cli: InvokeCliType,
    duckdb_example_project: Path,
    last_log_file_contents: t.Callable[[], str],
    create_site_package: CreateSitePackageType,
) -> None:
    """
    Scenario:
     - A model is created using a macro that is imported from an external package
     - That model is applied + snapshot committed to state
     - The external package is removed locally and the import macro import is changed to an inline definition
     - Thus, local version of the model can be rendered but the remote version in state cannot

    Outcome:
     - `sqlmesh plan --select-model <this model>` should work as it picks up the local version
     - `sqlmesh plan --select-model <some other model> should exit with an error, because the plan needs a valid DAG and the remote version is no longer valid locally
    """
    project_path = duckdb_example_project

    # simulate a 3rd party library that provides a macro
    package_name, package_path = create_site_package("sqlmesh_test_macros")
    (package_path / "macros.py").write_text("""
from sqlmesh import macro

@macro()
def do_something(evaluator):
    return "'value from site-packages'"
""")

    # reference the macro from site-packages
    (project_path / "macros" / "__init__.py").write_text(f"""
from {package_name}.macros import do_something
""")

    (project_path / "models" / "example.sql").write_text("""
MODEL (
    name sqlmesh_example.test_model,
    kind FULL
);

select @do_something() as a
""")

    result = invoke_cli(["plan", "--no-prompts", "--auto-apply", "--skip-tests"])

    assert result.returncode == 0
    assert "Physical layer updated" in result.stdout
    assert "Virtual layer updated" in result.stdout

    # clear cache to ensure we are forced to reload everything
    assert invoke_cli(["clean"]).returncode == 0

    # deleting this removes the 'do_something()' macro used by the version of the snapshot stored in state
    # when loading the old snapshot from state in the local python env, this will create an ImportError
    shutil.rmtree(package_path)

    # Move the macro inline so its no longer being loaded from a library but still exists with the same signature
    (project_path / "macros" / "__init__.py").write_text("""
from sqlmesh import macro

@macro()
def do_something(evaluator):
    return "'some value not from site-packages'"
""")

    # the invalid snapshot is in state but is not preventing a plan
    result = invoke_cli(
        [
            "plan",
            "--no-prompts",
            "--skip-tests",
        ],
        input="n",  # for the apply backfill (y/n) prompt
    )
    assert result.returncode == 0
    assert "Apply - Backfill Tables [y/n]:" in result.stdout
    assert "Physical layer updated" not in result.stdout

    # the invalid snapshot in state should not prevent a plan if --select-model is used on it (since the local version can be rendered)
    result = invoke_cli(
        ["plan", "--select-model", "sqlmesh_example.test_model", "--no-prompts", "--skip-tests"],
        input="n",  # for the apply backfill (y/n) prompt
    )
    assert result.returncode == 0
    assert "ModuleNotFoundError" not in result.stdout
    assert "sqlmesh_example.test_model" in result.stdout
    assert "Apply - Backfill Tables" in result.stdout

    # the invalid snapshot in state should prevent a plan if --select-model is used on another model
    # (since this says to SQLMesh "source everything from state except this selected model" and the plan DAG must be valid to run the plan)
    result = invoke_cli(
        [
            "plan",
            "--select-model",
            "sqlmesh_example.full_model",
            "--no-prompts",
            "--skip-tests",
        ],
        input="n",  # for the apply backfill (y/n) prompt
    )
    assert result.returncode == 1
    assert (
        "Model 'sqlmesh_example.test_model' sourced from state cannot be rendered in the local environment"
        in result.stdout
    )
    assert f"No module named '{package_name}'" in result.stdout
    assert (
        "If the model has been fixed locally, please ensure that the --select-model expression includes it"
        in result.stdout
    )

    # verify the full stack trace was logged
    log_file_contents = last_log_file_contents()
    assert f"ModuleNotFoundError: No module named '{package_name}'" in log_file_contents
    assert (
        "The above exception was the direct cause of the following exception:" in log_file_contents
    )


def test_model_selector_tags_picks_up_both_remote_and_local(
    invoke_cli: InvokeCliType, duckdb_example_project: Path
) -> None:
    """
    Scenario:
     - A model that has already been applied to prod (so exists in state) has a tag added locally
     - A new model is created locally that has the same tag

    Outcome:
     - `sqlmesh plan --select-model tag:<tag>` should include both models
    """
    project_path = duckdb_example_project

    # default state of full_model
    (project_path / "models" / "full_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.full_model,
        kind FULL,
        cron '@daily',
        grain item_id,
        audits (assert_positive_order_ids),
    );

    SELECT
        item_id,
        COUNT(DISTINCT id) AS num_orders
    FROM sqlmesh_example.incremental_model
    GROUP BY item_id
    """)

    # apply plan - starting point
    result = invoke_cli(["plan", "--no-prompts", "--auto-apply", "--skip-tests"])

    assert result.returncode == 0
    assert "Physical layer updated" in result.stdout
    assert "Virtual layer updated" in result.stdout

    # add a new model locally with tag:a
    (project_path / "models" / "new_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.new_model,
        kind full,
        tags (a)
    );

    SELECT 1;
    """)

    # update full_model with tag:a
    (project_path / "models" / "full_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.full_model,
        kind FULL,
        tags (a)
    );

    SELECT
        item_id,
        COUNT(DISTINCT id) AS num_orders
    FROM sqlmesh_example.incremental_model
    GROUP BY item_id
    """)

    result = invoke_cli(
        ["plan", "--select-model", "tag:a", "--no-prompts", "--skip-tests"],
        input="n",  # for the apply backfill (y/n) prompt
    )
    assert result.returncode == 0
    assert "sqlmesh_example.full_model" in result.stdout  # metadata update: tags
    assert "sqlmesh_example.new_model" in result.stdout  # added
