import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import model_validator


class MergeMethod(str, Enum):
    MERGE = "merge"
    SQUASH = "squash"
    REBASE = "rebase"


class GithubCICDBotConfig(BaseConfig):
    type_: t.Literal["github"] = Field(alias="type", default="github")

    invalidate_environment_after_deploy: bool = True
    enable_deploy_command: bool = False
    merge_method: t.Optional[MergeMethod] = None
    command_namespace: t.Optional[str] = None
    auto_categorize_changes: CategorizerConfig = CategorizerConfig.all_off()
    default_pr_start: t.Optional[TimeLike] = None
    skip_pr_backfill: bool = True
    pr_include_unmodified: t.Optional[bool] = None
    run_on_deploy_to_prod: bool = False
    pr_environment_name: t.Optional[str] = None
    prod_branch_names_: t.Optional[str] = Field(default=None, alias="prod_branch_name")

    @model_validator(mode="before")
    @classmethod
    def _validate(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        if data.get("enable_deploy_command") and not data.get("merge_method"):
            raise ValueError("merge_method must be set if enable_deploy_command is True")
        if data.get("command_namespace") and not data.get("enable_deploy_command"):
            raise ValueError("enable_deploy_command must be set if command_namespace is set")

        return data

    @property
    def prod_branch_names(self) -> t.List[str]:
        if self.prod_branch_names_:
            return [self.prod_branch_names_]
        return ["main", "master"]

    FIELDS_FOR_ANALYTICS: t.ClassVar[t.Set[str]] = {
        "invalidate_environment_after_deploy",
        "enable_deploy_command",
        "merge_method",
        "command_namespace",
        "auto_categorize_changes",
        "default_pr_start",
        "skip_pr_backfill",
        "pr_include_unmodified",
        "run_on_deploy_to_prod",
    }
