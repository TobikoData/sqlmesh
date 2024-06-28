import abc
import typing as t
from datetime import datetime

Interval = t.Tuple[datetime, datetime]


class Signal(abc.ABC):
    @abc.abstractmethod
    def poll_ready_intervals(self) -> bool | list[Interval]:
        """Poll the signal and return True if it is ready, otherwise return False."""


SignalFactory = t.Callable[[t.Dict[str, str | int | float | bool]], Signal]
