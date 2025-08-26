import pytest

from sqlmesh.dbt.project import Project

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
    
    assert helper.models()["top_waiters"].description == "description of top waiters"
    # Inline description in yaml
    top_waiters = sushi_test_project.context._models["top_waiters"]
    assert top_waiters.description == "description of top waiters"

    # Docs block from .md file
    waiters = sushi_test_project.context._models["waiters"]
    assert waiters.description == "waiters docs block"
