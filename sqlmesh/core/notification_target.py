from __future__ import annotations

import functools
import smtplib
import sys
import typing as t
from email.message import EmailMessage
from enum import Enum

from pydantic import Field, SecretStr

from sqlmesh.core.console import Console, get_console
from sqlmesh.integrations import slack
from sqlmesh.utils.errors import AuditError, ConfigError, MissingDependencyError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated

if t.TYPE_CHECKING:
    from slack_sdk import WebClient, WebhookClient


def _sqlmesh_version() -> str:
    try:
        from sqlmesh import __version__

        return __version__
    except ImportError:
        return "0.0.0"


NOTIFICATION_FUNCTIONS: t.Dict[NotificationEvent, str] = {}


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
    APPLY_END = "apply_end"
    RUN_START = "run_start"
    RUN_END = "run_end"
    MIGRATION_START = "migration_start"
    MIGRATION_END = "migration_end"
    APPLY_FAILURE = "apply_failure"
    RUN_FAILURE = "run_failure"
    AUDIT_FAILURE = "audit_failure"
    MIGRATION_FAILURE = "migration_failure"


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
        """Sends notification with the provided message.

        Args:
            notification_status: The status of the notification. One of: success, failure, warning, info, or progress.
            msg: The message to send.
        """

    @notify(NotificationEvent.APPLY_START)
    def notify_apply_start(self, environment: str) -> None:
        """Notify when an apply starts.

        Args:
            environment: The target environment of the plan.
        """
        self.send(NotificationStatus.INFO, f"Plan apply started for environment `{environment}`.")

    @notify(NotificationEvent.APPLY_END)
    def notify_apply_end(self, environment: str) -> None:
        """Notify when an apply ends.

        Args:
            environment: The target environment of the plan.
        """
        self.send(
            NotificationStatus.SUCCESS, f"Plan apply finished for environment `{environment}`."
        )

    @notify(NotificationEvent.RUN_START)
    def notify_run_start(self, environment: str) -> None:
        """Notify when a SQLMesh run starts.

        Args:
            environment: The target environment of the run.
        """
        self.send(NotificationStatus.INFO, f"SQLMesh run started for environment `{environment}`.")

    @notify(NotificationEvent.RUN_END)
    def notify_run_end(self, environment: str) -> None:
        """Notify when a SQLMesh run ends.

        Args:
            environment: The target environment of the run.
        """
        self.send(
            NotificationStatus.SUCCESS, f"SQLMesh run finished for environment `{environment}`."
        )

    @notify(NotificationEvent.MIGRATION_START)
    def notify_migration_start(self) -> None:
        """Notify when a SQLMesh migration starts."""
        self.send(NotificationStatus.INFO, "SQLMesh migration started.")

    @notify(NotificationEvent.MIGRATION_END)
    def notify_migration_end(self) -> None:
        """Notify when a SQLMesh migration ends."""
        self.send(NotificationStatus.SUCCESS, "SQLMesh migration finished.")

    @notify(NotificationEvent.APPLY_FAILURE)
    def notify_apply_failure(self, exc: str) -> None:
        """Notify in the case of an apply failure.

        Args:
            exc: The exception stack trace.
        """
        self.send(NotificationStatus.FAILURE, "Plan apply failed.", exc=exc)

    @notify(NotificationEvent.RUN_FAILURE)
    def notify_run_failure(self, exc: str) -> None:
        """Notify in the case of a run failure.

        Args:
            exc: The exception stack trace.
        """
        self.send(NotificationStatus.FAILURE, "SQLMesh run failed.", exc=exc)

    @notify(NotificationEvent.AUDIT_FAILURE)
    def notify_audit_failure(self, audit_error: AuditError) -> None:
        """Notify in the case of an audit failure.

        Args:
            audit_error: The AuditError object.
        """
        self.send(NotificationStatus.FAILURE, "Audit failure.", audit_error=audit_error)

    @notify(NotificationEvent.MIGRATION_FAILURE)
    def notify_migration_failure(self, exc: str) -> None:
        """Notify in the case of a migration failure.

        Args:
            exc: The exception stack trace.
        """
        self.send(NotificationStatus.FAILURE, "SQLMesh migration failed.", exc=exc)

    @property
    def is_configured(self) -> bool:
        return True


class BaseTextBasedNotificationTarget(BaseNotificationTarget):
    """
    A base class for unstructured notification targets (e.g.: console, email, etc.)
    """

    def send_text_message(self, notification_status: NotificationStatus, msg: str) -> None:
        """Send the notification message as text."""

    def send(
        self,
        notification_status: NotificationStatus,
        msg: str,
        audit_error: t.Optional[AuditError] = None,
        exc: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        error = None
        if audit_error:
            error = str(audit_error)
        elif exc:
            error = exc

        self.send_text_message(notification_status, msg if error is None else f"{msg}\n{error}")


class ConsoleNotificationTarget(BaseTextBasedNotificationTarget):
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

    def send_text_message(self, notification_status: NotificationStatus, msg: str) -> None:
        if notification_status.is_success:
            self.console.log_success(msg)
        elif notification_status.is_failure:
            self.console.log_error(msg)
        else:
            self.console.log_status_update(msg)


class BaseSlackNotificationTarget(BaseNotificationTarget):
    def send(
        self,
        notification_status: NotificationStatus,
        msg: str,
        audit_error: t.Optional[AuditError] = None,
        exc: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:

        status_emoji = {
            NotificationStatus.PROGRESS: slack.SlackAlertIcon.START,
            NotificationStatus.SUCCESS: slack.SlackAlertIcon.SUCCESS,
            NotificationStatus.FAILURE: slack.SlackAlertIcon.FAILURE,
            NotificationStatus.WARNING: slack.SlackAlertIcon.WARNING,
            NotificationStatus.INFO: slack.SlackAlertIcon.INFO,
        }

        composed = slack.message().add_primary_blocks(
            slack.header_block(f"{status_emoji[notification_status]} SQLMesh Notification"),
            slack.context_block(f"*Status:* `{notification_status.value}`"),
            slack.divider_block(),
            slack.text_section_block(f"*Message*: {msg}"),
        )

        details = []
        if audit_error:
            details = [
                slack.fields_section_block(
                    f"*Audit*: `{audit_error.audit_name}`",
                    f"*Model*: `{audit_error.model_name}`",
                    f"*Count*: `{audit_error.count}`",
                ),
                slack.preformatted_rich_text_block(audit_error.sql(pretty=True)),
            ]
        elif exc:
            details = [slack.preformatted_rich_text_block(exc)]

        composed.add_primary_blocks(
            *details,
            slack.divider_block(),
            slack.context_block(
                f"*SQLMesh Version:* {_sqlmesh_version()}", f"*Python Version:* {sys.version}"
            ),
        )

        self._send_slack_message(
            composed=composed.slack_message,
        )

    def _send_slack_message(self, composed: slack.TSlackMessage) -> None:
        """Send a composed message Slack.

        Args:
            composed: the formatted message to send to Slack
        """


class SlackWebhookNotificationTarget(BaseSlackNotificationTarget):
    url: t.Optional[str] = None
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

            if not self.url:
                raise ConfigError("Missing Slack webhook URL")

            self._client = WebhookClient(url=self.url)
        return self._client

    def _send_slack_message(self, composed: slack.TSlackMessage) -> None:
        self.client.send(
            blocks=composed["blocks"],
            attachments=composed["attachments"],  # type: ignore
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.url)


class SlackApiNotificationTarget(BaseSlackNotificationTarget):
    token: t.Optional[str] = None
    channel: t.Optional[str] = None
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

    def _send_slack_message(self, composed: slack.TSlackMessage) -> None:
        if not self.channel:
            raise ConfigError("Missing Slack channel for notification")

        self.client.chat_postMessage(
            channel=self.channel,
            blocks=composed["blocks"],
            attachments=composed["attachments"],  # type: ignore
        )

    @property
    def is_configured(self) -> bool:
        return all((self.token, self.channel))


class BasicSMTPNotificationTarget(BaseTextBasedNotificationTarget):
    host: t.Optional[str] = None
    port: int = 465
    user: t.Optional[str] = None
    password: t.Optional[SecretStr] = None
    sender: t.Optional[str] = None
    recipients: t.Optional[t.FrozenSet[str]] = None
    subject: t.Optional[str] = "SQLMesh Notification"
    type_: Literal["smtp"] = Field(alias="type", default="smtp")

    def send_text_message(
        self,
        notification_status: NotificationStatus,
        msg: str,
    ) -> None:
        if not self.host:
            raise ConfigError("Missing SMTP host for notification")

        email = EmailMessage()
        email["Subject"] = self.subject
        email["To"] = ",".join(self.recipients or [])
        email["From"] = self.sender
        email.set_content(msg)
        with smtplib.SMTP_SSL(host=self.host, port=self.port) as smtp:
            if self.user and self.password:
                smtp.login(user=self.user, password=self.password.get_secret_value())
            smtp.send_message(email)

    @property
    def is_configured(self) -> bool:
        return all((self.host, self.user, self.password, self.sender))


NotificationTarget = Annotated[
    t.Union[
        BasicSMTPNotificationTarget,
        ConsoleNotificationTarget,
        SlackApiNotificationTarget,
        SlackWebhookNotificationTarget,
    ],
    Field(discriminator="type_"),
]


class NotificationTargetManager:
    """Wrapper around a list of notification targets.

    Calling a notification target's "notify_" method on this object will call it
    on all registered notification targets.
    """

    def __init__(
        self,
        notification_targets: t.Dict[NotificationEvent, t.Set[NotificationTarget]] | None = None,
        user_notification_targets: t.Dict[str, t.Set[NotificationTarget]] | None = None,
        username: str | None = None,
    ) -> None:
        self.notification_targets = notification_targets or {}
        self.user_notification_targets = user_notification_targets or {}
        self.username = username

    def notify(self, event: NotificationEvent, *args: t.Any, **kwargs: t.Any) -> None:
        """Call the 'notify_`event`' function of all notification targets that care about the event."""
        if self.username:
            self.notify_user(event, self.username, *args, **kwargs)
        else:
            for notification_target in self.notification_targets.get(event, set()):
                notify_func = self._get_notification_function(notification_target, event)
                notify_func(*args, **kwargs)

    def notify_user(
        self, event: NotificationEvent, username: str, *args: t.Any, **kwargs: t.Any
    ) -> None:
        """Call the 'notify_`event`' function of the user's notification targets that care about the event."""
        notification_targets = self.user_notification_targets.get(username, set())
        for notification_target in notification_targets:
            if event in notification_target.notify_on:
                notify_func = self._get_notification_function(notification_target, event)
                notify_func(*args, **kwargs)

    def _get_notification_function(
        self, notification_target: NotificationTarget, event: NotificationEvent
    ) -> t.Callable:
        """Lookup the registered function for a notification event"""
        func_name = NOTIFICATION_FUNCTIONS[event]
        return getattr(notification_target, func_name)
