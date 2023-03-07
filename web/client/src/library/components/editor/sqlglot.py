import json

import sqlglot


def parseToJSON(sql: str) -> str:
    return json.dumps(sqlglot.parse(sql))


[sqlglot.transpile, parseToJSON]
