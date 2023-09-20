from sqlglot.dialects.dialect import DialectType

# Sizes based on a composite key/index of two text fields with 4 bytes per characters.
MAX_TEXT_INDEX_LENGTH = {
    "mysql": "380",  # 380 characters per column, <= 3070 byte index size limit
    "tsql": "450",  # 450 bytes per column, <= 900 byte index size limit
}


def index_text_type(dialect: DialectType) -> str:
    """
    MySQL and MSSQL cannot create indexes or primary keys on TEXT fields; they
    require that the fields have a VARCHAR type of fixed length.

    This helper abstracts away the type of such fields.
    """

    return (
        f"VARCHAR({MAX_TEXT_INDEX_LENGTH[str(dialect)]})"
        if dialect in MAX_TEXT_INDEX_LENGTH
        else "TEXT"
    )
