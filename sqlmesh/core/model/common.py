from __future__ import annotations

import typing as t

from sqlglot.expressions import split_num_words


def parse_model_name(name: str) -> t.Tuple[t.Optional[str], t.Optional[str], str]:
    """Convert a model name into table parts.

    Args:
        name: model name.

    Returns:
        A tuple consisting of catalog, schema, table name.
    """
    return split_num_words(name, ".", 3)  # type: ignore
