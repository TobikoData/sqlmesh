from __future__ import annotations

import hashlib
import typing as t
import zlib


def crc32(data: t.Iterable[t.Optional[str]]) -> str:
    return str(zlib.crc32(_safe_concat(data)))


def md5(data: t.Iterable[t.Optional[str]]) -> str:
    return hashlib.md5(_safe_concat(data)).hexdigest()


def hash_data(data: t.Iterable[t.Optional[str]]) -> str:
    return crc32(data)


def _safe_concat(data: t.Iterable[t.Optional[str]]) -> bytes:
    return ";".join("" if d is None else d for d in data).encode("utf-8")
