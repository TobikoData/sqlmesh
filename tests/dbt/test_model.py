import pytest

from sqlmesh.dbt.common import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.model import ModelConfig
from sqlmesh.dbt.test import TestConfig
from sqlmesh.utils.errors import ConfigError

pytestmark = pytest.mark.dbt


def test_model_test_circular_references() -> None:
    upstream_model = ModelConfig(name="upstream")
    downstream_model = ModelConfig(name="downstream", dependencies=Dependencies(refs={"upstream"}))
    context = DbtContext(_refs={"upstream": upstream_model, "downstream": downstream_model})

    # Test and downstream model references
    downstream_test = TestConfig(
        name="downstream_with_upstream",
        sql="",
        dependencies=Dependencies(refs={"upstream", "downstream"}),
    )
    upstream_test = TestConfig(
        name="upstream_with_downstream",
        sql="",
        dependencies=Dependencies(refs={"upstream", "downstream"}),
    )
    downstream_model.tests = [downstream_test]
    downstream_model.check_for_circular_test_refs(context)

    downstream_model.tests = []
    upstream_model.tests = [upstream_test]
    with pytest.raises(ConfigError, match="downstream model"):
        upstream_model.check_for_circular_test_refs(context)

    downstream_model.tests = [downstream_test]
    with pytest.raises(ConfigError, match="downstream model"):
        upstream_model.check_for_circular_test_refs(context)
    downstream_model.check_for_circular_test_refs(context)

    # Test only references
    downstream_model.dependencies = Dependencies()
    with pytest.raises(ConfigError, match="between tests"):
        upstream_model.check_for_circular_test_refs(context)
    with pytest.raises(ConfigError, match="between tests"):
        downstream_model.check_for_circular_test_refs(context)
