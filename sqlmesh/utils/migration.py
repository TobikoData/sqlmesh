from sqlglot.dialects.dialect import DialectType

# This limit accommodates a composite key which consists of two text fields
# with 4 bytes per characters and a 3070 bytes limit on the key size.
MYSQL_MAX_TEXT_INDEX_LENGTH = 380


def index_text_type(dialect: DialectType) -> str:
    """
    MySQL cannot create indexes or primary keys on TEXT fields; it requires that
    the fields have a VARCHAR type of fixed length. This helper simply abstracts
    away the type of such fields.
    """
    return f"VARCHAR({MYSQL_MAX_TEXT_INDEX_LENGTH})" if dialect == "mysql" else "TEXT"
