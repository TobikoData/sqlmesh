import subprocess
from pathlib import Path
import pytest
from sqlmesh.utils.git import GitClient


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo_path, check=True, capture_output=True)
    return repo_path


def test_git_uncommitted_changes(git_repo: Path):
    git_client = GitClient(git_repo)

    test_file = git_repo / "model.sql"
    test_file.write_text("SELECT 1 AS a")
    subprocess.run(["git", "add", "model.sql"], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Max",
            "-c",
            "user.email=max@rb.com",
            "commit",
            "-m",
            "Initial commit",
        ],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )
    assert git_client.list_uncommitted_changed_files() == []

    # make an unstaged change and see that it is listed
    test_file.write_text("SELECT 2 AS a")
    uncommitted = git_client.list_uncommitted_changed_files()
    assert len(uncommitted) == 1
    assert uncommitted[0].name == "model.sql"

    # stage the change and test that it is still detected
    subprocess.run(["git", "add", "model.sql"], cwd=git_repo, check=True, capture_output=True)
    uncommitted = git_client.list_uncommitted_changed_files()
    assert len(uncommitted) == 1
    assert uncommitted[0].name == "model.sql"


def test_git_both_staged_and_unstaged_changes(git_repo: Path):
    git_client = GitClient(git_repo)

    file1 = git_repo / "model1.sql"
    file2 = git_repo / "model2.sql"
    file1.write_text("SELECT 1")
    file2.write_text("SELECT 2")
    subprocess.run(["git", "add", "."], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Max",
            "-c",
            "user.email=max@rb.com",
            "commit",
            "-m",
            "Initial commit",
        ],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )

    # stage file1
    file1.write_text("SELECT 10")
    subprocess.run(["git", "add", "model1.sql"], cwd=git_repo, check=True, capture_output=True)

    # modify file2 but don't stage it!
    file2.write_text("SELECT 20")

    # both should be detected
    uncommitted = git_client.list_uncommitted_changed_files()
    assert len(uncommitted) == 2
    file_names = {f.name for f in uncommitted}
    assert file_names == {"model1.sql", "model2.sql"}


def test_git_untracked_files(git_repo: Path):
    git_client = GitClient(git_repo)
    initial_file = git_repo / "initial.sql"
    initial_file.write_text("SELECT 0")
    subprocess.run(["git", "add", "initial.sql"], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Max",
            "-c",
            "user.email=max@rb.com",
            "commit",
            "-m",
            "Initial commit",
        ],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )

    new_file = git_repo / "new_model.sql"
    new_file.write_text("SELECT 1")

    # untracked file should not appear in uncommitted changes
    assert git_client.list_uncommitted_changed_files() == []

    # but in untracked
    untracked = git_client.list_untracked_files()
    assert len(untracked) == 1
    assert untracked[0].name == "new_model.sql"


def test_git_committed_changes(git_repo: Path):
    git_client = GitClient(git_repo)

    test_file = git_repo / "model.sql"
    test_file.write_text("SELECT 1")
    subprocess.run(["git", "add", "model.sql"], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Max",
            "-c",
            "user.email=max@rb.com",
            "commit",
            "-m",
            "Initial commit",
        ],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )

    subprocess.run(
        ["git", "checkout", "-b", "feature"],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )

    test_file.write_text("SELECT 2")
    subprocess.run(["git", "add", "model.sql"], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Max",
            "-c",
            "user.email=max@rb.com",
            "commit",
            "-m",
            "Update on feature branch",
        ],
        cwd=git_repo,
        check=True,
        capture_output=True,
    )

    committed = git_client.list_committed_changed_files(target_branch="main")
    assert len(committed) == 1
    assert committed[0].name == "model.sql"

    assert git_client.list_uncommitted_changed_files() == []
