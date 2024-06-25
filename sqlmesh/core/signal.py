import abc
import typing as t


class Signal(abc.ABC):
    @abc.abstractmethod
    def wait(self) -> None:
        """Wait for the signal to be ready. This method should block until the signal becomes ready."""


SignalFactory = t.Callable[[t.Dict[str, str | int | float | bool]], Signal]
