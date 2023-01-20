from __future__ import annotations

import sys
import typing as t

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated

from pydantic import Field
from sqlglot import exp

from sqlmesh.core.notification_target import ConsoleNotificationTarget
from sqlmesh.integrations.github.notification_target import GithubNotificationTarget

NotificationTarget = Annotated[
    t.Union[ConsoleNotificationTarget, GithubNotificationTarget],
    Field(discriminator="type_"),
]

if t.TYPE_CHECKING:
    TableName = t.Union[str, exp.Table]
