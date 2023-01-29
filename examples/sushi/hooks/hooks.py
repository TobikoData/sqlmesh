import typing as t
from datetime import datetime

from sqlmesh import ExecutionContext, hook


@hook()
def noop(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    latest: datetime,
    **kwargs: t.Any,
) -> None:
    pass
