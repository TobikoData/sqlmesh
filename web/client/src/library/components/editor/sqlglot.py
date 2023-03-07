import json

import sqlglot
from sqlglot.dialects.dialect import DialectType as DialectType


def parse_to_json(sql: str, read: DialectType = None) -> str:
    return json.dumps([exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read)])


[sqlglot.transpile, parse_to_json]
