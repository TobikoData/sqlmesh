from __future__ import annotations

import typing as t
from datetime import datetime

from sqlglot import exp

if t.TYPE_CHECKING:
    TableName = t.Union[str, exp.Table]
    SchemaName = t.Union[str, exp.Table]
    SessionProperties = t.Dict[str, t.Union[exp.Expression, str, int, float, bool]]
    CustomMaterializationProperties = t.Dict[str, t.Union[exp.Expression, str, int, float, bool]]
    Interval = t.Tuple[datetime, datetime]
    Batch = t.List[Interval]
