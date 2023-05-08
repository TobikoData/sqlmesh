from __future__ import annotations

import typing as t


def ensure_bool(val: t.Any) -> bool:
    if isinstance(val, bool):
        return val

    if isinstance(val, str):
        val = try_str_to_bool(val)

    return bool(val)


def try_str_to_bool(val: str) -> t.Union[str, bool]:
    maybe_bool = val.lower()
    if maybe_bool in ["true", "false"]:
        return maybe_bool == "true"

    return val
