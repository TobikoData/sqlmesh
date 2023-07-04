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
        "keywords": " ".join(keywords),
        "types": " ".join(type_mapping.get(t, t.value) for t in exp.DataType.Type),
    }

    return json.dumps(output)


def format(sql: str = "", read: DialectType = None) -> str:
    output = "\n".join(
        sqlglot.transpile(sql, read=read, error_level=sqlglot.errors.ErrorLevel.IGNORE, pretty=True)
    )

    return output


def validate(sql: str = "", read: DialectType = None) -> str:
    is_valid = True
    try:
        sqlglot.transpile(
            sql, read=read, pretty=False, unsupported_level=sqlglot.errors.ErrorLevel.IMMEDIATE
        )
    except sqlglot.errors.ParseError as e:
        is_valid = False

    return json.dumps(is_valid)


{
    "dialects": dialects,
    "get_dialect": get_dialect,
    "parse_to_json": parse_to_json,
    "validate": validate,
    "format": format,
}
