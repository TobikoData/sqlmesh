from __future__ import annotations

import sys
import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.core.console import Console, get_console
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class NotificationStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    INFO = "info"
    PROGRESS = "progress"

    @property
    def is_success(self) -> bool:
        return self == NotificationStatus.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self == NotificationStatus.FAILURE

    @property
    def is_info(self) -> bool:
        return self == NotificationStatus.INFO

    @property
    def is_warning(self) -> bool:
        return self == NotificationStatus.WARNING

    @property
    def is_progress(self) -> bool:
        return self == NotificationStatus.PROGRESS


class BaseNotificationTarget(PydanticModel):
    """
    Base notification target model. Provides a command for sending notifications that is currently only used
    by the built-in scheduler. Other schedulers like Airflow use the configuration of the target itself
    to create the notification constructs appropriate for the scheduler.
    """

    type_: str

    def send(self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any) -> None:
        """
        Sends notification with the provided message. Currently only used by the built-in scheduler.
        """


class ConsoleNotificationTarget(BaseNotificationTarget):
    """
    Example console notification target. Keeping this around for testing purposes.
    """

    type_: Literal["console"] = Field(alias="type", default="console")
    _console: t.Optional[Console] = None

    @property
    def console(self) -> Console:
        if not self._console:
            self._console = get_console()
        return self._console

    def send(self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any) -> None:
        if notification_status.is_success:
            self.console.log_success(msg)
        elif notification_status.is_failure:
            self.console.log_error(msg)
        else:
            self.console.log_status_update(msg)
