from pathlib import Path
import pytest

from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.profile import Profile


pytestmark = pytest.mark.dbt


@pytest.mark.xdist_group("dbt_manifest")
def test_docs_inline():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        model_defaults=ModelDefaultsConfig(start="2020-01-01"),
    )
    # Inline description in yaml
    assert helper.models()["waiters"].description == "waiters docs block"
    # Docs block from .md file
    assert helper.models()["top_waiters"].description == "description of top waiters"
