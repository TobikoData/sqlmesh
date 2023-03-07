import json

import sqlglot


def parseToJSON(sql: str) -> str:
    return json.dumps([exp.dump() if exp else {} for exp in sqlglot.parse(sql)])


[sqlglot.transpile, parseToJSON]
