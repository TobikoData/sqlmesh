import json
import typing as t

import sqlglot
from sqlglot import Tokenizer, exp
from sqlglot.dialects.dialect import Dialect, DialectType


def parse_to_json(sql: str, read: DialectType = None) -> str:
    return json.dumps([exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read)])


def get_dialect(dialect_type: t.Optional[str] = None) -> str:
    tokenizer = Tokenizer

    if dialect_type in Dialect.classes:
        dialect: t.Any = Dialect.classes[dialect_type]
        tokenizer = dialect.Tokenizer

    return json.dumps(
        {
            "keywords": " ".join(list(tokenizer.KEYWORDS)) + " ",
            "types": " ".join([t.value for t in exp.DataType.Type]) + " ",
        }
    )


def get_dialects() -> str:
    return json.dumps(list(Dialect.classes))


[sqlglot.transpile, parse_to_json, get_dialect, get_dialects]
