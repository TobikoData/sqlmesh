import pytest

from sqlmesh.core.reference import Graph
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def make_model(mocker):
    def make(name, refs = None):
        mock = mocker.Mock()
        mock.name = name
        references = []
        for ref_name, unique in (refs or []):
            mock_ref = mocker.Mock()
            mock_ref.name = ref_name
            mock_ref.unique = unique
            references.append(mock_ref)
        mock.all_references = references
        return mock

    return make


def test_graph(make_model):
    graph = Graph([
        make_model("model_a", [("a", True), ("b", False), ("c", False), ("d", False)]),
        make_model("model_b", [("a", True)]),
        make_model("model_c", [("d", True), ("e", True)]),
        make_model("model_d", [("e", True)]),
        make_model("model_e", [("b", False)]),
    ])

    def find_path(a, b):
        return [(model, r.name) for model, r in graph.find_path(a, b)]

    assert find_path("model_a", "model_b") == [("model_a", "a"), ("model_b", "a")]
    assert find_path("model_a", "model_d") == [('model_a', 'd'), ('model_c', 'e'), ('model_d', 'e')]

    with pytest.raises(SQLMeshError):
        assert find_path("model_a", "model_e")
