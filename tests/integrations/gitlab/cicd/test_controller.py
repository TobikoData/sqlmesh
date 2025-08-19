import typing as t

from pytest_mock.plugin import MockerFixture

from sqlmesh.integrations.gitlab.cicd.controller import GitlabController, GitlabCommitStatus


def test_controller_init(mocker: MockerFixture, make_controller: t.Callable, gitlab_client: t.Any):
    controller = make_controller({}, gitlab_client)
    assert isinstance(controller, GitlabController)


def test_pr_environment_name(
    mocker: MockerFixture, make_controller: t.Callable, gitlab_client: t.Any
):
    controller = make_controller({}, gitlab_client)
    assert controller.pr_environment_name == "sushi_1"


def test_pr_targets_prod_branch(
    mocker: MockerFixture, make_controller: t.Callable, gitlab_client: t.Any
):
    controller = make_controller({}, gitlab_client)
    assert controller.pr_targets_prod_branch

    controller._merge_request.target_branch = "dev"
    assert not controller.pr_targets_prod_branch


def test_update_sqlmesh_comment_info(
    mocker: MockerFixture, make_controller: t.Callable, gitlab_client: t.Any
):
    controller = make_controller({}, gitlab_client)
    controller.update_sqlmesh_comment_info("test comment")
    controller._merge_request.notes.create.assert_called_once_with(
        {"body": ":robot: **SQLMesh Bot Info** :robot:\ntest comment"}
    )


def test_update_check(mocker: MockerFixture, make_controller: t.Callable, gitlab_client: t.Any):
    controller = make_controller({}, gitlab_client)
    controller._update_check(
        "test_check", GitlabCommitStatus.SUCCESS, "description", "http://target.url"
    )
    gitlab_client.projects.get.return_value.commits.get.return_value.statuses.create.assert_called_once_with(
        {
            "name": "test_check",
            "status": "success",
            "description": "description",
            "target_url": "http://target.url",
        }
    )
