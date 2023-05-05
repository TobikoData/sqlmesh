from sqlglot import exp

from sqlmesh.core.macros import macro


@macro()
def mask(evaluator, column):
    """
    Creates a one-way hash of the column in order to allow it to be consistent/joinable while removing
    sensitive information

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@mask('sensitive_col')"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'MD5(sensitive_col)'
    """
    return exp.func("MD5", column)
