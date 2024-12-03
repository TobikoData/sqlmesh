import typing as t

from sqlmesh import signal, DatetimeRanges


@signal()
def test_signal(batch: DatetimeRanges, arg: int = 0) -> t.Union[bool, DatetimeRanges]:
    assert arg == 1
    return True
