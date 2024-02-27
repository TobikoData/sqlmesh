from __future__ import annotations

import typing as t

from sqlglot import exp

if t.TYPE_CHECKING:
    TableName = t.Union[str, exp.Table]
    SchemaName = t.Union[str, exp.Table]
    SessionProperties = t.Dict[str, t.Union[exp.Expression, str, int, float, bool]]
