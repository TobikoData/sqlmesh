import typing as t
from enum import Enum

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

    @property
    def is_required_approver(self) -> bool:
        """Indicates if this is a required approver for PR approvals."""
        return UserRole.REQUIRED_APPROVER in self.roles
