import typing as t

from sqlmesh.utils.pydantic import PydanticModel


class User(PydanticModel):
    """SQLMesh user information that can be used for notifications"""

    username: str
    """The name to refer to the user"""
    github_login_username: t.Optional[str] = None
    """The github login username"""
    slack_username: t.Optional[str] = None
    """The slack username"""
    email: t.Optional[str] = None
    """The email for the user (full address)"""
    is_gatekeeper: bool = False
    """Indicates if this is a gatekeeper for PR approvals.
    TODO: Users should be able to define this on a "per-project" level but that requires adding the concept of 
    "projects" to SQLMesh so making this a global config for now 
    """
