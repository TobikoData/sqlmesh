from __future__ import annotations

import typing as t
from datetime import date, datetime


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


def make_serializable(obj: t.Any) -> t.Any:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_serializable(item) for item in obj]
    return obj
