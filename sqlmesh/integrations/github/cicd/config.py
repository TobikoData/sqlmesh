import sys
import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import model_validator, model_validator_v1_args

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


class MergeMethod(str, Enum):
    MERGE = "merge"
    SQUASH = "squash"
    REBASE = "rebase"


class GithubCICDBotConfig(BaseConfig):
    type_: Literal["github"] = Field(alias="type", default="github")

    invalidate_environment_after_deploy: bool = True
    enable_deploy_command: bool = False
    merge_method: t.Optional[MergeMethod] = None
    command_namespace: t.Optional[str] = None
    auto_categorize_changes: CategorizerConfig = CategorizerConfig.all_off()
    default_pr_start: t.Optional[TimeLike] = None
    skip_pr_backfill: bool = True
    pr_include_unmodified: t.Optional[bool] = None
    run_on_deploy_to_prod: bool = True

    @model_validator(mode="before")
    @model_validator_v1_args
    def _validate(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        if values.get("enable_deploy_command") and not values.get("merge_method"):
            raise ValueError("merge_method must be set if enable_deploy_command is True")
        if values.get("command_namespace") and not values.get("enable_deploy_command"):
            raise ValueError("enable_deploy_command must be set if command_namespace is set")
        return values
