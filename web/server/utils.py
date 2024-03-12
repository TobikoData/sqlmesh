from __future__ import annotations

import asyncio
import functools
import io
import typing as t
from pathlib import Path, PurePath

import pandas as pd
import pyarrow as pa  # type: ignore
from fastapi import Depends, HTTPException
from starlette.responses import StreamingResponse
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_context

R = t.TypeVar("R")


class ArrowStreamingResponse(StreamingResponse):
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        kwargs["media_type"] = "application/vnd.apache.arrow.stream"
        super().__init__(*args, **kwargs)


async def run_in_executor(func: t.Callable[..., R], *args: t.Any) -> R:
    """Run in the default loop's executor"""

    @functools.wraps(func)
    def func_wrapper() -> R:
        try:
            return func(*args)
        except ApiException as e:
            api_console.log_exception(e)
            raise e
        except Exception as e:
            api_console.log_exception(
                ApiException(
                    message="An unexpected error occurred",
                    origin="API -> utils -> run_in_executor",
                    trigger="An unexpected error occurred",
                    status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                )
            )
            raise e

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func_wrapper)


def validate_path(
    path: str,
    context: Context = Depends(get_context),
) -> str:
    resolved_path = context.path.resolve()
    full_path = (resolved_path / path).resolve()
    try:
        full_path.relative_to(resolved_path)
    except ValueError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    if any(
        full_path.match(pattern) for pattern in context.config_for_path(Path(path)).ignore_patterns
    ):
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    return path


def replace_file(src: Path, dst: Path) -> None:
    """Move a file or directory at src to dst."""
    if src != dst:
        try:
            src.replace(dst)
        except FileNotFoundError:
            raise HTTPException(status_code=HTTP_404_NOT_FOUND)
        except OSError:
            raise ApiException(
                message="Unable to move a file",
                origin="API -> utils -> replace_file",
            )


def df_to_pyarrow_bytes(df: pd.DataFrame) -> io.BytesIO:
    """Convert a DataFrame to pyarrow bytes stream"""
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)

    return io.BytesIO(sink.getvalue().to_pybytes())


def is_relative_to(path: PurePath, other: PurePath | str) -> bool:
    """Return whether or not path is relative to the other path."""
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False
