from __future__ import annotations

import typing as t
import zlib


def crc32(data: t.Iterable[t.Optional[str]]) -> str:
    return str(zlib.crc32(";".join("" if d is None else d for d in data).encode("utf-8")))


def hash_data(data: t.Iterable[t.Optional[str]]) -> str:
    return crc32(data)
