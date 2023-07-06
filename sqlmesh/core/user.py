import typing as t
from enum import Enum

from pydantic import validator

from sqlmesh.core.notification_target import (
    BasicSMTPNotificationTarget,
    NotificationTarget,
)
from sqlmesh.utils.pydantic import PydanticModel


class UserRole(str, Enum):
    """A role to associate the user with"""

    REQUIRED_APPROVER = "required_approver"

    @property
    def is_required_approver(self) -> bool:
        return self == UserRole.REQUIRED_APPROVER


class User(PydanticModel):
    """SQLMesh user information that can be used for notifications"""

    username: str
    """The name to refer to the user"""
    github_username: t.Optional[str] = None
    """The github login username"""
    slack_username: t.Optional[str] = None
    """The slack username"""
    email: t.Optional[str] = None
    """The email for the user (full address)"""
    roles: t.List[UserRole] = []
    """List of roles to associate with the user"""
    notification_targets: t.List[NotificationTarget] = []
    """List of notification targets"""

    @property
    def is_required_approver(self) -> bool:
        """Indicates if this is a required approver for PR approvals."""
        return UserRole.REQUIRED_APPROVER in self.roles

    @validator("notification_targets")
    def validate_notification_targets(
        cls,
        v: t.List[NotificationTarget],
        values: t.Dict[str, t.Any],
    ) -> t.List[NotificationTarget]:
        for target in v:
            if isinstance(target, BasicSMTPNotificationTarget) and target.recipients != {
                values["email"]
            }:
                raise ValueError("Recipient emails do not match user email")
        return v
