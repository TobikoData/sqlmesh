from __future__ import annotations

import subprocess
import typing as t
from functools import cached_property
from pathlib import Path


class GitClient:
    def __init__(self, repo: str | Path):
        self._work_dir = Path(repo)

    def list_untracked_files(self) -> t.List[Path]:
        return self._execute_list_output(
            ["ls-files", "--others", "--exclude-standard"], self._work_dir
        )

    def list_uncommitted_changed_files(self) -> t.List[Path]:
        return self._execute_list_output(["diff", "--name-only", "--diff-filter=d"], self._git_root)

    def list_committed_changed_files(self, target_branch: str = "main") -> t.List[Path]:
        return self._execute_list_output(
            ["diff", "--name-only", "--diff-filter=d", f"{target_branch}..."], self._git_root
        )

    def _execute_list_output(self, commands: t.List[str], base_path: Path) -> t.List[Path]:
        return [(base_path / o).absolute() for o in self._execute(commands).split("\n") if o]

    def _execute(self, commands: t.List[str]) -> str:
        result = subprocess.run(
            ["git"] + commands,
            cwd=self._work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        # If the Git command failed, extract and raise the error message in the console
        if result.returncode != 0:
            stderr_output = result.stderr.decode("utf-8").strip()
            error_message = next(
                (line for line in stderr_output.splitlines() if line.lower().startswith("fatal:")),
                stderr_output,
            )
            raise RuntimeError(f"Git error: {error_message}")

        return result.stdout.decode("utf-8").strip()

    @cached_property
    def _git_root(self) -> Path:
        return Path(self._execute(["rev-parse", "--show-toplevel"]))
