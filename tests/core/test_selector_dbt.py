import typing as t
import pytest
from pytest_mock import MockerFixture
from sqlglot import exp
from sqlmesh.core.model.kind import SeedKind, ExternalKind, FullKind
from sqlmesh.core.model.seed import Seed
from sqlmesh.core.model.definition import SqlModel, SeedModel, ExternalModel
from sqlmesh.core.audit.definition import StandaloneAudit
from sqlmesh.core.snapshot.definition import Node
from sqlmesh.core.selector import DbtSelector
from sqlmesh.core.selector import parse, ResourceType
from sqlmesh.utils.errors import SQLMeshError
import sqlmesh.core.dialect as d
from sqlmesh.utils import UniqueKeyDict


def test_parse_resource_type():
    assert parse("resource_type:foo") == ResourceType(this=exp.Var(this="foo"))


@pytest.mark.parametrize(
    "resource_type,expected",
    [
        ("model", {'"test"."normal_model"'}),
        ("seed", {'"test"."seed_model"'}),
        ("test", {'"test"."standalone_audit"'}),
        ("source", {'"external"."model"'}),
    ],
)
def test_expand_model_selections_resource_type(
    mocker: MockerFixture, resource_type: str, expected: t.Set[str]
):
    models: t.Dict[str, Node] = {
        '"test"."normal_model"': SqlModel(
            name="test.normal_model",
            kind=FullKind(),
            query=d.parse_one("SELECT 'normal_model' AS what"),
        ),
        '"test"."seed_model"': SeedModel(
            name="test.seed_model", kind=SeedKind(path="/tmp/foo"), seed=Seed(content="id,name")
        ),
        '"test"."standalone_audit"': StandaloneAudit(
            name="test.standalone_audit", query=d.parse_one("SELECT 'standalone_audit' AS what")
        ),
        '"external"."model"': ExternalModel(name="external.model", kind=ExternalKind()),
    }

    selector = DbtSelector(state_reader=mocker.Mock(), models=UniqueKeyDict("models"))

    assert selector.expand_model_selections([f"resource_type:{resource_type}"], models) == expected


def test_unsupported_resource_type(mocker: MockerFixture):
    selector = DbtSelector(state_reader=mocker.Mock(), models=UniqueKeyDict("models"))

    models: t.Dict[str, Node] = {
        '"test"."normal_model"': SqlModel(
            name="test.normal_model", query=d.parse_one("SELECT 'normal_model' AS what")
        ),
    }

    with pytest.raises(SQLMeshError, match="Unsupported"):
        selector.expand_model_selections(["resource_type:analysis"], models)
