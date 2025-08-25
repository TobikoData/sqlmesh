import pytest

from sqlmesh.dbt.project import Project

pytestmark = pytest.mark.dbt


def test_docs_inline(sushi_test_project: Project):
    # Inline description in yaml
    top_waiters = sushi_test_project.context._models["top_waiters"]
    assert top_waiters.description == "description of top waiters"

    # Docs block from .md file
    waiters = sushi_test_project.context._models["waiters"]
    assert waiters.description == "waiters docs block"
