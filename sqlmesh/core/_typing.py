from __future__ import annotations

import typing as t

from sqlglot import exp

if t.TYPE_CHECKING:
    TableName = t.Union[str, exp.Table]
