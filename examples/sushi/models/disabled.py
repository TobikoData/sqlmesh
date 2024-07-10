import typing as t
from sqlmesh import ExecutionContext, model


@model(
    "sushi.disabled_py",
    columns={
        "id": "int",
    },
    enabled=False,
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> None:
    return None
