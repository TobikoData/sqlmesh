from sqlglot import expressions

from sqlmesh.utils import columns_to_types_all_known


def test_columns_to_types_all_known() -> None:
    assert (
        columns_to_types_all_known(
            {"a": expressions.DataType.build("INT"), "b": expressions.DataType.build("INT")}
        )
        == True
    )
    assert (
        columns_to_types_all_known(
            {"a": expressions.DataType.build("UNKNOWN"), "b": expressions.DataType.build("INT")}
        )
        == False
    )
    assert (
        columns_to_types_all_known(
            {"a": expressions.DataType.build("NULL"), "b": expressions.DataType.build("INT")}
        )
        == False
    )
