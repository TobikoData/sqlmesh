import os
import pathlib
import typing as t
from unittest import mock

import pytest
import time_machine
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.config import CategorizerConfig, Config, ModelDefaultsConfig, LinterConfig
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.integrations.gitlab.cicd.config import GitlabCICDBotConfig
from sqlmesh.core.cicd.config import MergeMethod
from sqlmesh.integrations.gitlab.cicd.controller import (
    GitlabCommitStatus,
    GitlabController,
    run_all_checks,
)
from sqlmesh.utils.errors import CICDBotError
from tests.integrations.gitlab.cicd.conftest import MockMergeRequestComment

pytestmark = [
    pytest.mark.slow,
    pytest.mark.gitlab,
]


def get_environment_objects(controller: GitlabController, environment: str) -> t.List[DataObject]:
    return controller._context.engine_adapter.get_data_objects(f"sushi__{environment}")


def get_num_days_loaded(controller: GitlabController, environment: str, model: str) -> int:
    return int(
        controller._context.engine_adapter.fetchdf(
            f"SELECT distinct event_date FROM sushi__{environment}.{model}"
        ).count()
    )


def get_columns(
    controller: GitlabController, environment: t.Optional[str], model: str
) -> t.Dict[str, exp.DataType]:
    table = f"sushi__{environment}.{model}" if environment else f"sushi.{model}"
    return controller._context.engine_adapter.columns(table)


@time_machine.travel("2023-01-01 15:00:00 UTC")
def test_linter(
    gitlab_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,  # This will need to be adapted for GitLab merge request comments
    make_merge_request_review,  # This will need to be adapted for GitLab merge request approvals
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    MR with the Linter enabled will contain a new check with the linter specific output.

    Scenarios:
    - MR with linter errors leads to job failures & skips
    - MR with linter warnings leads to job successes
    """
    mock_project = gitlab_client.get_project()
    mock_project.create_commit_status = mocker.MagicMock(
        side_effect=lambda **kwargs: make_mock_check_run(**kwargs)
    )

    created_comments: t.List[MockMergeRequestComment] = []
    mock_merge_request = mock_project.mergerequests.get()
    mock_merge_request.notes.create = mocker.MagicMock(
        side_effect=lambda comment: make_mock_issue_comment(  # This needs to be adapted
            comment=comment, created_comments=created_comments
        )
    )
    mock_merge_request.notes.list = mocker.MagicMock(side_effect=lambda: created_comments)

    mock_merge_request.approvals.get = mocker.MagicMock(
        side_effect=lambda: make_merge_request_review(
            username="test_gitlab", state="approved"
        )  # This needs to be adapted
    )
    mock_merge_request.merged = False
    mock_merge_request.merge = mocker.MagicMock()

    # Case 1: Test for linter errors
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        linter=LinterConfig(enabled=True, rules="ALL"),
    )

    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_synchronized.json",  # This fixture needs to be created
        gitlab_client,
        bot_config=GitlabCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
        config=config,
    )

    gitlab_output_file = tmp_path / "gitlab_output.txt"

    with mock.patch.dict(
        os.environ, {"GITLAB_OUTPUT": str(gitlab_output_file)}
    ):  # This env var needs to be confirmed for GitLab
        with pytest.raises(CICDBotError):
            run_all_checks(controller)

    assert "SQLMesh - Linter" in controller._check_run_mapping
    linter_checks_runs = controller._check_run_mapping["SQLMesh - Linter"].all_kwargs
    assert (
        "Linter **errors** for" in linter_checks_runs[2]["output"]["summary"]
    )  # This output structure needs to be confirmed for GitLab
    assert GitlabCommitStatus(
        linter_checks_runs[2]["conclusion"]
    ).is_failure  # This needs to be adapted for GitLab

    for check in (
        "SQLMesh - PR Environment Synced",
        "SQLMesh - Prod Plan Preview",
    ):
        assert check in controller._check_run_mapping
        check_runs = controller._check_run_mapping[check].all_kwargs
        assert GitlabCommitStatus(
            check_runs[-1]["conclusion"]
        ).is_skipped  # This needs to be adapted for GitLab

    with open(gitlab_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "linter=failure\nrun_unit_tests=success\npr_environment_name=hello_world_2\npr_environment_synced=skipped\nprod_plan_preview=skipped\n"  # This output needs to be confirmed for GitLab
        )

    # empty gitlab file for next case
    open(gitlab_output_file, "w").close()

    # Case 2: Test for linter warnings
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        linter=LinterConfig(enabled=True, warn_rules="ALL"),
    )

    controller = make_controller(
        "tests/fixtures/gitlab/merge_request_synchronized.json",  # This fixture needs to be created
        gitlab_client,
        bot_config=GitlabCICDBotConfig(
            merge_method=MergeMethod.MERGE,
            invalidate_environment_after_deploy=False,
            auto_categorize_changes=CategorizerConfig.all_full(),
            default_pr_start=None,
            skip_pr_backfill=False,
        ),
        mock_out_context=False,
        config=config,
    )

    with mock.patch.dict(
        os.environ, {"GITLAB_OUTPUT": str(gitlab_output_file)}
    ):  # This env var needs to be confirmed for GitLab
        run_all_checks(controller)

    assert "SQLMesh - Linter" in controller._check_run_mapping
    linter_checks_runs = controller._check_run_mapping["SQLMesh - Linter"].all_kwargs
    assert (
        "Linter warnings for" in linter_checks_runs[-1]["output"]["summary"]
    )  # This output structure needs to be confirmed for GitLab
    assert GitlabCommitStatus(
        linter_checks_runs[-1]["conclusion"]
    ).is_success  # This needs to be adapted for GitLab

    for check in (
        "SQLMesh - Run Unit Tests",
        "SQLMesh - PR Environment Synced",
        "SQLMesh - Prod Plan Preview",
    ):
        assert check in controller._check_run_mapping
        check_runs = controller._check_run_mapping[check].all_kwargs
        assert GitlabCommitStatus(
            check_runs[-1]["conclusion"]
        ).is_success  # This needs to be adapted for GitLab

    with open(gitlab_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "linter=success\nrun_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\n"  # This output needs to be confirmed for GitLab
        )
