import pytest
from click.testing import CliRunner
from pytest_mock.plugin import MockerFixture

from sqlmesh.cli.main import cli
from sqlmesh.integrations.gitlab.cicd.controller import BotCommand


def test_gitlab_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["gitlab"])
    assert result.exit_code == 0


def test_run_all_checks_command(mocker: MockerFixture):
    mock_controller = mocker.MagicMock()
    mock_run_all_checks = mocker.patch("sqlmesh.integrations.gitlab.cicd.command.run_all_checks")
    mocker.patch(
        "sqlmesh.integrations.gitlab.cicd.command.GitlabController",
        return_value=mock_controller,
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["gitlab", "run-all-checks", "--token", "abc"])

    assert result.exit_code == 0
    mock_run_all_checks.assert_called_once_with(mock_controller)


@pytest.mark.gitlab
def test_run_deploy_command_command(mocker: MockerFixture):
    mock_controller = mocker.MagicMock()
    mock_controller.get_command_from_comment.return_value = BotCommand.DEPLOY_PROD
    mock_run_deploy_command = mocker.patch(
        "sqlmesh.integrations.gitlab.cicd.command.run_deploy_command"
    )
    mocker.patch(
        "sqlmesh.integrations.gitlab.cicd.command.GitlabController",
        return_value=mock_controller,
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["gitlab", "run-deploy-command", "--token", "abc"])

    assert result.exit_code == 0
    mock_run_deploy_command.assert_called_once_with(mock_controller)
