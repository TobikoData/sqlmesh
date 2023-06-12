import json

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType

dialects = json.dumps(
    [
        {
            "dialect_title": "SQLGlot" if dialect == "" else dialect_class.__name__,
            "dialect_name": dialect,
        }
        for dialect, dialect_class in Dialect.classes.items()
    ]
)


def parse_to_json(sql: str, read: DialectType = None) -> str:
    return json.dumps(
        [exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read, error_level="ignore")]
    )


def get_dialect(name: str = "") -> str:
    dialect = Dialect.classes.get(name, Dialect)
    keywords = dialect.tokenizer_class.KEYWORDS
    type_mapping = dialect.generator_class.TYPE_MAPPING

    output = {
        "keywords": " ".join(keywords) + " ",
        "types": " ".join(type_mapping.get(t, t.value) for t in exp.DataType.Type) + " ",
    }

    return json.dumps(output)


[parse_to_json, get_dialect, dialects]
