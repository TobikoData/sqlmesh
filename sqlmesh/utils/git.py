from __future__ import annotations

import subprocess
import typing as t
from functools import cached_property
from pathlib import Path


class GitClient:
    def __init__(self, repo: str | Path):
        self._work_dir = Path(repo)

    def list_untracked_files(self) -> t.List[Path]:
        return self._execute_list_output(["ls-files", "--others", "--exclude-standard"])

    def list_changed_files(self, target_branch: str = "main") -> t.List[Path]:
        return self._execute_list_output(["diff", "--name-only", "--diff-filter=d", target_branch])

    def _execute_list_output(self, commands: t.List[str]) -> t.List[Path]:
        return [(self._git_root / o).absolute() for o in self._execute(commands).split("\n") if o]

    def _execute(self, commands: t.List[str]) -> str:
        result = subprocess.run(["git"] + commands, cwd=self._work_dir, stdout=subprocess.PIPE)
        return result.stdout.decode("utf-8").strip()

    @cached_property
    def _git_root(self) -> Path:
        return Path(self._execute(["rev-parse", "--show-toplevel"]))
