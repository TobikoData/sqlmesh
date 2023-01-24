from sqlglot import expressions as exp

from sqlmesh.core.macros import macro, MacroEvaluator


@macro()
def mask(
    evaluator: MacroEvaluator,
    column: exp.Column,
) -> exp.Anonymous:
    """
    Creates a one-way hash of the column in order to allow it to be consistent/joinable while removing
    sensitive information

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@mask('sensitive_col')"
        >>> MacroEvaluator("duckdb", {"MASK": mask}).transform(parse_one(sql)).sql()
        'MD5(sensitive_col)'
    """
    return exp.Anonymous(this="md5", expressions=[column])
