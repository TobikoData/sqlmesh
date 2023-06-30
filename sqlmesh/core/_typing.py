from __future__ import annotations

import typing as t

from sqlglot import exp

from sqlmesh.core.notification_target import BaseNotificationTarget

NotificationTarget = BaseNotificationTarget

if t.TYPE_CHECKING:
    TableName = t.Union[str, exp.Table]
