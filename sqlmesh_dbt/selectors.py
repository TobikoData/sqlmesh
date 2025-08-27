import typing as t
import logging

logger = logging.getLogger(__name__)


def to_sqlmesh(dbt_select: t.Collection[str], dbt_exclude: t.Collection[str]) -> t.Optional[str]:
    """
    Given selectors defined in the format of the dbt cli --select and --exclude arguments, convert them into a selector expression that
    the SQLMesh selector engine can understand.

    The main things being mapped are:
        - set union (" " between items within the same selector string OR multiple --select arguments) is mapped to " | "
        - set intersection ("," between items within the same selector string) is mapped to " & "
        - `--exclude`. The SQLMesh selector engine does not treat this as a separate parameter and rather treats exclusion as a normal selector
          that just happens to contain negation syntax, so we generate these by negating each expression and then intersecting the result
          with any --select expressions

    Things that are *not* currently being mapped include:
        - selectors based on file paths
        - selectors based on partially qualified names like "model_a". The SQLMesh selector engine requires either:
            - wildcards, eg "*model_a*"
            - the full model name qualified with the schema, eg "staging.model_a"

    Examples:
        --select "model_a"
            -> "model_a"
        --select "main.model_a"
            -> "main.model_a"
        --select "main.model_a" --select "main.model_b"
            -> "main.model_a | main.model_b"
        --select "main.model_a main.model_b"
            -> "main.model_a | main.model_b"
        --select "(main.model_a+ & ^main.model_b)"
            -> "(main.model_a+ & ^main.model_b)"
        --select "+main.model_a" --exclude "raw.src_data"
            -> "+main.model_a & ^(raw.src_data)"
        --select "+main.model_a" --select "main.*b+" --exclude "raw.src_data"
            -> "(+main.model_a | main.*b+) & ^(raw.src_data)"
        --select "+main.model_a" --select "main.*b+" --exclude "raw.src_data" --exclude "main.model_c"
            -> "(+main.model_a | main.*b+) & ^(raw.src_data | main.model_c)"
        --select "+main.model_a main.*b+" --exclude "raw.src_data main.model_c"
            -> "(+main.model_a | main.*b+) & ^(raw.src_data | main.model_c)"
    """
    if not dbt_select and not dbt_exclude:
        return None

    select_expr = " | ".join(_to_sqlmesh(expr) for expr in dbt_select)
    select_expr = _wrap(select_expr) if dbt_exclude and len(dbt_select) > 1 else select_expr

    exclude_expr = ""

    if dbt_exclude:
        exclude_expr = " | ".join(_to_sqlmesh(expr) for expr in dbt_exclude)
        exclude_expr = _negate(
            _wrap(exclude_expr) if dbt_select and len(dbt_exclude) > 1 else exclude_expr
        )

    main_expr = " & ".join([expr for expr in [select_expr, exclude_expr] if expr])

    logger.debug(
        f"Expanded dbt select: {dbt_select}, exclude: {dbt_exclude} into SQLMesh: {main_expr}"
    )

    return main_expr


def _to_sqlmesh(selector_str: str) -> str:
    unions, intersections = _split_unions_and_intersections(selector_str)

    union_expr = " | ".join(unions)
    intersection_expr = " & ".join(intersections)

    if len(unions) > 1 and intersections:
        union_expr = f"({union_expr})"

    if len(intersections) > 1 and unions:
        intersection_expr = f"({intersection_expr})"

    return " | ".join([expr for expr in [union_expr, intersection_expr] if expr])


def _split_unions_and_intersections(selector_str: str) -> t.Tuple[t.List[str], t.List[str]]:
    # break space-separated items like: "my_first_model my_second_model" into a list of selectors to union
    # and comma-separated items like: "my_first_model,my_second_model" into a list of selectors to intersect
    # but, take into account brackets, eg "(my_first_model & my_second_model)" should not be split
    # also take into account both types in the same string, eg "my_first_model my_second_model model_3,model_4,model_5"

    def _split_by(input: str, delimiter: str) -> t.Iterator[str]:
        buf = ""
        depth = 0

        for char in input:
            if char == delimiter and depth <= 0:
                # only split on a space if we are not within parenthesis
                yield buf
                buf = ""
                continue
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1

            buf += char

        if buf:
            yield buf

    # first, break up based on spaces
    segments = list(_split_by(selector_str, " "))

    # then, within each segment, identify the unions and intersections
    unions = []
    intersections = []

    for segment in segments:
        maybe_intersections = list(_split_by(segment, ","))
        if len(maybe_intersections) > 1:
            intersections.extend(maybe_intersections)
        else:
            unions.append(segment)

    return unions, intersections


def _negate(expr: str) -> str:
    return f"^{_wrap(expr)}"


def _wrap(expr: str) -> str:
    already_wrapped = expr.strip().startswith("(") and expr.strip().endswith(")")

    if expr and not already_wrapped:
        return f"({expr})"

    return expr
