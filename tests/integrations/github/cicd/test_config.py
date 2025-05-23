import pathlib

import pytest

from sqlmesh.core.config import (
    AutoCategorizationMode,
    CategorizerConfig,
    Config,
    load_config_from_paths,
)
from sqlmesh.utils.errors import ConfigError
from sqlmesh.integrations.github.cicd.config import MergeMethod
from tests.utils.test_filesystem import create_temp_file

pytestmark = pytest.mark.github


def test_load_yaml_config_default(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: github
model_defaults:
    dialect: duckdb
""",
    )
    config = load_config_from_paths(
        Config,
        project_paths=[tmp_path / "config.yaml"],
    )
    assert config.cicd_bot.type_ == "github"
    assert config.cicd_bot.invalidate_environment_after_deploy
    assert config.cicd_bot.merge_method is None
    assert config.cicd_bot.command_namespace is None
    assert config.cicd_bot.auto_categorize_changes == CategorizerConfig.all_off()
    assert config.cicd_bot.default_pr_start is None
    assert not config.cicd_bot.enable_deploy_command
    assert config.cicd_bot.skip_pr_backfill
    assert config.cicd_bot.pr_include_unmodified is None
    assert config.cicd_bot.pr_environment_name is None
    assert config.cicd_bot.prod_branch_names == ["main", "master"]


def test_load_yaml_config(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: github
    invalidate_environment_after_deploy: false
    merge_method: squash
    command_namespace: "#SQLMesh"
    auto_categorize_changes:
      external: full
      python: full
      sql: full
      seed: full
    default_pr_start: 
    enable_deploy_command: true
    skip_pr_backfill: false
    pr_include_unmodified: true
    pr_environment_name: "MyOverride"
    prod_branch_name: testing
model_defaults:
    dialect: duckdb
""",
    )
    config = load_config_from_paths(
        Config,
        project_paths=[tmp_path / "config.yaml"],
    )
    assert config.cicd_bot.type_ == "github"
    assert not config.cicd_bot.invalidate_environment_after_deploy
    assert config.cicd_bot.merge_method == MergeMethod.SQUASH
    assert config.cicd_bot.command_namespace == "#SQLMesh"
    assert config.cicd_bot.auto_categorize_changes == CategorizerConfig(
        external=AutoCategorizationMode.FULL,
        python=AutoCategorizationMode.FULL,
        sql=AutoCategorizationMode.FULL,
        seed=AutoCategorizationMode.FULL,
    )
    assert config.cicd_bot.default_pr_start is None
    assert config.cicd_bot.enable_deploy_command
    assert not config.cicd_bot.skip_pr_backfill
    assert config.cicd_bot.pr_include_unmodified
    assert config.cicd_bot.pr_environment_name == "MyOverride"
    assert config.cicd_bot.prod_branch_names == ["testing"]


def test_load_python_config_defaults(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.core.config import Config, ModelDefaultsConfig

config = Config(
    cicd_bot=GithubCICDBotConfig(),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
""",
    )
    config = load_config_from_paths(
        Config,
        project_paths=[tmp_path / "config.py"],
    )
    assert config.cicd_bot.type_ == "github"
    assert config.cicd_bot.invalidate_environment_after_deploy
    assert config.cicd_bot.merge_method is None
    assert config.cicd_bot.command_namespace is None
    assert config.cicd_bot.auto_categorize_changes == CategorizerConfig.all_off()
    assert config.cicd_bot.default_pr_start is None
    assert not config.cicd_bot.enable_deploy_command
    assert config.cicd_bot.skip_pr_backfill
    assert config.cicd_bot.pr_include_unmodified is None
    assert config.cicd_bot.pr_environment_name is None
    assert config.cicd_bot.prod_branch_names == ["main", "master"]


def test_load_python_config(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.py"),
        """
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig, Config, ModelDefaultsConfig

config = Config(
    cicd_bot=GithubCICDBotConfig(
        invalidate_environment_after_deploy=False,
        merge_method=MergeMethod.SQUASH,
        command_namespace="#SQLMesh",
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.FULL,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL,
        ),
        default_pr_start="1 week ago",
        enable_deploy_command=True,
        skip_pr_backfill=False,
        pr_include_unmodified=True,
        pr_environment_name="MyOverride",
        prod_branch_name="testing",
    ),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
""",
    )

    config = load_config_from_paths(
        Config,
        project_paths=[tmp_path / "config.py"],
    )
    assert config.cicd_bot.type_ == "github"
    assert not config.cicd_bot.invalidate_environment_after_deploy
    assert config.cicd_bot.merge_method == MergeMethod.SQUASH
    assert config.cicd_bot.command_namespace == "#SQLMesh"
    assert config.cicd_bot.auto_categorize_changes == CategorizerConfig(
        external=AutoCategorizationMode.FULL,
        python=AutoCategorizationMode.FULL,
        sql=AutoCategorizationMode.FULL,
        seed=AutoCategorizationMode.FULL,
    )
    assert config.cicd_bot.default_pr_start == "1 week ago"
    assert config.cicd_bot.enable_deploy_command
    assert not config.cicd_bot.skip_pr_backfill
    assert config.cicd_bot.pr_include_unmodified
    assert config.cicd_bot.pr_environment_name == "MyOverride"
    assert config.cicd_bot.prod_branch_names == ["testing"]


def test_validation(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: github
    command_namespace: "#SQLMesh"
    enable_deploy_command: False
model_defaults:
    dialect: duckdb
""",
    )
    with pytest.raises(
        ConfigError, match=r".*enable_deploy_command must be set if command_namespace is set.*"
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
cicd_bot:
    type: github
    enable_deploy_command: True
model_defaults:
    dialect: duckdb
""",
    )
    with pytest.raises(
        ConfigError, match=r".*merge_method must be set if enable_deploy_command is True.*"
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])


def test_ttl_in_past(tmp_path):
    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
environment_ttl: in 1 week
model_defaults:
    dialect: duckdb
""",
    )

    config = load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])
    assert config.environment_ttl == "in 1 week"

    create_temp_file(
        tmp_path,
        pathlib.Path("config.yaml"),
        """
environment_ttl: 1 week
model_defaults:
    dialect: duckdb
""",
    )
    with pytest.raises(
        ConfigError,
        match=r".*TTL '1 week' is in the past. Please specify a relative time in the future. Ex: `in 1 week` instead of `1 week`.*",
    ):
        load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])

        create_temp_file(
            tmp_path,
            pathlib.Path("config.yaml"),
            """
snapshot_ttl: 1 week
model_defaults:
    dialect: duckdb
    """,
        )
        with pytest.raises(
            ValueError,
            match="TTL '1 week' is in the past. Please specify a relative time in the future. Ex: `in 1 week` instead of `1 week`.",
        ):
            load_config_from_paths(Config, project_paths=[tmp_path / "config.yaml"])
