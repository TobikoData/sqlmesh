from sqlglot.dialects.dialect import DialectType

# This is an ad-hoc upper bound which doesn't necessarily match MySQL's
# TEXT storage specification, but it should be good for most use cases.
MAX_TEXT_LENGTH = 1024


def index_text_type(dialect: DialectType) -> str:
    """
    MySQL cannot create indexes or primary keys on TEXT fields; it requires that
    the fields have a VARCHAR type of fixed length. This helper simply abstracts
    away the type of such fields.
    """
    return f"VARCHAR({MAX_TEXT_LENGTH})" if dialect == "mysql" else "TEXT"
