from sqlglot.dialects.dialect import DialectType

# https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings
MAX_TEXT_LENGTH = 65535


def primary_key_text_type(dialect: DialectType) -> str:
    """
    MySQL cannot create indexes or primary keys on TEXT fields; it requires that
    the fields have a VARCHAR type of fixed length. This helper simply abstracts
    away the type of such fields.
    """
    return f"VARCHAR({MAX_TEXT_LENGTH})" if dialect == "mysql" else "TEXT"
