# import json
# import pathlib
# import typing as t
#
# import pytest
# from pytest_mock.plugin import MockerFixture
#
# from sqlmesh.integrations.github.pr_bot import has_gatekeeper_approval
#
#
# def get_payload(name: str) -> t.Dict[str, t.Any]:
#     path = pathlib.Path(
#         pathlib.Path(__file__).parent.resolve(), "fixtures", f"{name}.json"
#     )
#     return json.loads(path.read_text())
#
#
# @pytest.fixture(scope="function", autouse=True)
# def pull_request_review_payload() -> t.Dict[str, t.Any]:
#     return get_payload("pull_request_review")
#
#
# def test_not_approval_review(
#     mocker: MockerFixture, pull_request_review_payload: t.Dict[str, t.Any]
# ):
#     pull_request_review_payload["review"]["state"] = "SUBMITTED"
#     mocker.patch(
#         "sqlmesh.integrations.github.pr_bot._get_payload",
#         return_value=pull_request_review_payload,
#     )
#     assert not has_gatekeeper_approval(pull_request_review_payload)
#
#
# def test_has_approval_not_gatekeeper(
#     mocker: MockerFixture, pull_request_review_payload: t.Dict[str, t.Any]
# ):
#     pull_request_review_payload["review"]["state"] = "APPROVED"
#     pull_request_review_payload["review"]["user"]["login"] = "NOT_GATEKEEPER"
#     mocker.patch(
#         "sqlmesh.integrations.github.pr_bot._get_payload",
#         return_value=pull_request_review_payload,
#     )
#     mocker.patch(
#         "sqlmesh.integrations.github.pr_bot.GATEKEEPER_GITHUB_LOGINS", {"gatekeeper"}
#     )
#     assert not has_gatekeeper_approval(pull_request_review_payload)
#
#
# def test_has_approval_is_gatekeeper(
#     mocker: MockerFixture, pull_request_review_payload: t.Dict[str, t.Any]
# ):
#     pull_request_review_payload["review"]["state"] = "APPROVED"
#     pull_request_review_payload["review"]["user"]["login"] = "GATEKEEPER"
#     mocker.patch(
#         "sqlmesh.integrations.github.pr_bot._get_payload",
#         return_value=pull_request_review_payload,
#     )
#     mocker.patch(
#         "sqlmesh.integrations.github.pr_bot.GATEKEEPER_GITHUB_LOGINS", {"gatekeeper"}
#     )
#     assert has_gatekeeper_approval(pull_request_review_payload)
