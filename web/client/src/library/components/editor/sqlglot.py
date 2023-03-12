import json
import typing as t

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType


def parse_to_json(sql: str, read: DialectType = None) -> str:
    return json.dumps([exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read)])


def get_dialect(dialect_type: str = "") -> str:
    dialect: t.Any = Dialect.classes.get(dialect_type, Dialect)

    tokenizer: t.Any = dialect.tokenizer_class
    type_mapping: t.Any = dialect.generator_class.TYPE_MAPPING

    return json.dumps(
        {
            "keywords": " ".join(tokenizer.KEYWORDS) + " ",
            "types": " ".join(type_mapping.get(t, t.value) for t in exp.DataType.Type) + " ",
        }
    )


def get_dialects() -> str:
    dialects = list()

    for name, dialect in Dialect.classes.items():
        dialects.append({"text": "SqlGlot" if name == "" else dialect.__name__, "value": name})

    return json.dumps(dialects)


[sqlglot.transpile, parse_to_json, get_dialect, get_dialects]
