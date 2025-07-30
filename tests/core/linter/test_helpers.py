from sqlmesh import Context
from sqlmesh.core.linter.helpers import read_range_from_file, get_range_of_model_block
from sqlmesh.core.model import SqlModel


def test_get_position_of_model_block():
    context = Context(paths=["examples/sushi"])

    sql_models = [
        model
        for model in context.models.values()
        if isinstance(model, SqlModel)
        and model._path is not None
        and str(model._path).endswith(".sql")
    ]
    assert len(sql_models) > 0

    for model in sql_models:
        dialect = model.dialect
        assert dialect is not None

        path = model._path
        assert path is not None

        with open(path, "r", encoding="utf-8") as file:
            content = file.read()

        as_lines = content.splitlines()

        range = get_range_of_model_block(content, dialect)
        assert range is not None

        #  Check that the range starts with MODEL and ends with ;
        read_range = read_range_from_file(path, range)
        assert read_range.startswith("MODEL")
        assert read_range.endswith(";")
