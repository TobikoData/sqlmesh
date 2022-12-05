from __future__ import annotations

import sys
import typing as t

from sqlmesh.core.console import Console, get_console
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class BaseNotificationTarget(PydanticModel):
    """
    Base notification target model. Provides a command for sending notifications that is currently only used
    by the built-in scheduler. Other schedulers like Airflow use the configuration of the target itself
    to create the notification constructs appropriate for the scheduler.
    """

    kind: str

    def send(self, msg: str, **kwargs) -> None:
        """
        Sends notification with the provided message. Currently only used by the built-in scheduler.
        """


class ConsoleNotificationTarget(BaseNotificationTarget):
    """
    Example console notification target. Keeping this around for testing purposes.
    """

    kind: Literal["console"] = "console"
    _console: t.Optional[Console] = None

    @property
    def console(self):
        if not self._console:
            self._console = get_console()
        return self._console

    def send(self, msg: str, **kwargs) -> None:
        self.console.log_status_update(msg)
