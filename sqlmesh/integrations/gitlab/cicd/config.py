import typing as t

from pydantic import Field

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.console import get_console


from sqlmesh.core.cicd.config import MergeMethod


class GitlabCICDBotConfig(BaseConfig):
    type_: t.Literal["gitlab"] = Field(alias="type", default="gitlab")

    project_id: t.Optional[str] = None
    invalidate_environment_after_deploy: bool = True
    enable_deploy_command: bool = False
    merge_method: t.Optional[MergeMethod] = None
    command_namespace: t.Optional[str] = None
    auto_categorize_changes_: t.Optional[CategorizerConfig] = Field(
        default=None, alias="auto_categorize_changes"
    )
    default_pr_start: t.Optional[TimeLike] = None
    skip_pr_backfill_: t.Optional[bool] = Field(default=None, alias="skip_pr_backfill")
    pr_include_unmodified_: t.Optional[bool] = Field(default=None, alias="pr_include_unmodified")
    run_on_deploy_to_prod: bool = False
    pr_environment_name: t.Optional[str] = None
    pr_min_intervals: t.Optional[int] = None
    prod_branch_names_: t.Optional[str] = Field(default=None, alias="prod_branch_name")
    forward_only_branch_suffix_: t.Optional[str] = Field(
        default=None, alias="forward_only_branch_suffix"
    )

    @property
    def prod_branch_names(self) -> t.List[str]:
        if self.prod_branch_names_:
            return [self.prod_branch_names_]
        return ["main", "master"]

    @property
    def auto_categorize_changes(self) -> CategorizerConfig:
        return self.auto_categorize_changes_ or CategorizerConfig.all_off()

    @property
    def pr_include_unmodified(self) -> bool:
        return self.pr_include_unmodified_ or False

    @property
    def skip_pr_backfill(self) -> bool:
        if self.skip_pr_backfill_ is None:
            get_console().log_warning(
                "`skip_pr_backfill` is unset, defaulting it to `true` (no data will be backfilled).\n"
                "Future versions of SQLMesh will default to `skip_pr_backfill: false` to align with the CLI default behaviour.\n"
                "If you would like to preserve the current behaviour and remove this warning, please explicitly set `skip_pr_backfill: true` in the bot config.\n\n"
                "For more information on configuring the bot, see: https://sqlmesh.readthedocs.io/en/stable/integrations/gitlab/"
            )
            return True
        return self.skip_pr_backfill_

    @property
    def forward_only_branch_suffix(self) -> str:
        return self.forward_only_branch_suffix_ or "-forward-only"

    FIELDS_FOR_ANALYTICS: t.ClassVar[t.Set[str]] = {
        "invalidate_environment_after_deploy",
        "enable_deploy_command",
        "auto_categorize_changes",
        "default_pr_start",
        "skip_pr_backfill",
        "pr_include_unmodified",
        "run_on_deploy_to_prod",
        "pr_min_intervals",
        "forward_only_branch_suffix",
    }
