from sqlglot import exp

from sqlmesh.core.macros import MacroEvaluator, macro


@macro()
def mask(
    evaluator: MacroEvaluator,
    column: exp.Column,
) -> exp.Expression:
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
    return evaluator.parse_one(f"MD5({column})")
