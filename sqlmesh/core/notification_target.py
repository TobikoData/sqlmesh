from __future__ import annotations

import functools
import smtplib
import sys
import typing as t
from email.message import EmailMessage
from enum import Enum

from pydantic import EmailStr, Field, SecretStr

from sqlmesh.core.console import Console, get_console
from sqlmesh.utils.errors import AuditError, MissingDependencyError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

if t.TYPE_CHECKING:
    from slack_sdk import WebClient, WebhookClient

NOTIFICATION_FUNCTIONS: dict[NotificationEvent, str] = {}


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


class NotificationEvent(str, Enum):
    APPLY_START = "apply_start"
    RUN_START = "run_start"
    APPLY_FAILURE = "apply_failure"
    RUN_FAILURE = "run_failure"
    AUDIT_FAILURE = "audit_failure"


def notify(event: NotificationEvent) -> t.Callable:
    """Decorator used to register 'notify' methods and the events they correspond to."""

    def decorator(f: t.Callable) -> t.Callable:
        @functools.wraps(f)
        def wrapper(*args: t.List[t.Any], **kwargs: t.Dict[str, t.Any]) -> None:
            return f(*args, **kwargs)

        NOTIFICATION_FUNCTIONS[event] = f.__name__
        return wrapper

    return decorator


class BaseNotificationTarget(PydanticModel, frozen=True):
    """
    Base notification target model. Provides a command for sending notifications that is currently only used
    by the built-in scheduler. Other schedulers like Airflow use the configuration of the target itself
    to create the notification constructs appropriate for the scheduler.
    """

    type_: str
    notify_on: t.FrozenSet[NotificationEvent] = frozenset()

    def send(self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any) -> None:
        """
        Sends notification with the provided message.
        """

    @notify(NotificationEvent.APPLY_START)
    def notify_apply_start(self, environment: str) -> None:
        """Notify when an apply starts."""
        self.send(NotificationStatus.INFO, f"Plan apply started for environment `{environment}`.")

    @notify(NotificationEvent.RUN_START)
    def notify_run_start(self, environment: str) -> None:
        """Notify when an apply starts."""
        self.send(NotificationStatus.INFO, f"SQLMesh run started for environment `{environment}`.")

    @notify(NotificationEvent.APPLY_FAILURE)
    def notify_apply_failure(self, exc: Exception) -> None:
        """Notify in the case of an apply failure."""
        self.send(NotificationStatus.FAILURE, f"Failed to apply plan.\n{exc}")

    @notify(NotificationEvent.RUN_FAILURE)
    def notify_run_failure(self, exc: Exception) -> None:
        """Notify in the case of a run failure."""
        self.send(NotificationStatus.FAILURE, "Failed to run SQLMesh.\n{exc}")

    @notify(NotificationEvent.AUDIT_FAILURE)
    def notify_audit_failure(self, audit_error: AuditError) -> None:
        """Notify in the case of an audit failure."""
        self.send(NotificationStatus.FAILURE, str(audit_error))


class NotificationTargetManager:
    """Wrapper around a list of notification targets.

    Calling a notification target's "notify_" method on this object will call it
    on all registered notification targets.
    """

    def __init__(
        self, notification_targets: dict[NotificationEvent, set[BaseNotificationTarget]]
    ) -> None:
        self.notification_targets = notification_targets

    def notify(self, event: NotificationEvent, *args: t.Any, **kwargs: t.Any) -> None:
        """Call the 'notify_`event`' function of all notification targets that care about the event."""
        for notification_target in self.notification_targets[event]:
            func_name = NOTIFICATION_FUNCTIONS[event]
            notify_func = getattr(notification_target, func_name)
            notify_func(*args, **kwargs)


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


class SlackWebhookNotificationTarget(BaseNotificationTarget):
    url: str
    type_: Literal["slack_webhook"] = Field(alias="type", default="slack_webhook")
    _client: t.Optional[WebhookClient] = None

    @property
    def client(self) -> WebhookClient:
        if not self._client:
            try:
                from slack_sdk import WebhookClient
            except ModuleNotFoundError as e:
                raise MissingDependencyError(
                    "Missing Slack dependencies. Run `pip install 'sqlmesh[slack]'` to install them."
                ) from e

            self._client = WebhookClient(url=self.url)
        return self._client

    def send(self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any) -> None:
        self.client.send(text=msg)


class SlackApiNotificationTarget(BaseNotificationTarget):
    token: str
    channel: str
    type_: Literal["slack_api"] = Field(alias="type", default="slack_api")
    _client: t.Optional[WebClient] = None

    @property
    def client(self) -> WebClient:
        if not self._client:
            try:
                from slack_sdk import WebClient
            except ModuleNotFoundError as e:
                raise MissingDependencyError(
                    "Missing Slack dependencies. Run `pip install 'sqlmesh[slack]'` to install them."
                ) from e

            self._client = WebClient(token=self.token)
        return self._client

    def send(self, notification_status: NotificationStatus, msg: str, **kwargs: t.Any) -> None:
        self.client.chat_postMessage(channel=self.channel, text=msg)


class BasicSMTPNotificationTarget(BaseNotificationTarget):
    host: str
    port: int = 465
    user: t.Optional[str] = None
    password: t.Optional[SecretStr] = None
    sender: EmailStr
    recipients: t.FrozenSet[EmailStr]
    subject: t.Optional[str] = "SQLMesh Notification"
    type_: Literal["smtp"] = Field(alias="type", default="smtp")

    def send(
        self,
        notification_status: NotificationStatus,
        msg: str,
        subject: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        email = EmailMessage()
        email["Subject"] = subject or self.subject
        email["To"] = ",".join(self.recipients)
        email["From"] = self.sender
        email.set_content(msg)
        with smtplib.SMTP_SSL(host=self.host, port=self.port) as smtp:
            if self.user and self.password:
                smtp.login(user=self.user, password=self.password.get_secret_value())
            smtp.send_message(email)
