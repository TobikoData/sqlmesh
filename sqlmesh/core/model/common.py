import typing as t


def parse_model_name(name: str) -> t.Tuple[t.Optional[str], t.Optional[str], str]:
    """Convert a model name into table parts.

    Args:
        name: model name.

    Returns:
        A tuple consisting of catalog, schema, table name.
    """
    splits = name.split(".")
    if len(splits) == 3:
        return (splits[0], splits[1], splits[2])
    if len(splits) == 2:
        return (None, splits[0], splits[1])
    return (None, None, name)
