from __future__ import annotations

import typing as t

from pydantic import Field

from sqlmesh.core.config.base import BaseConfig


class FormatOptions(BaseConfig):
    """The format options for SQL code.

    Args:
        append_newline: Whether to append a newline to the end of the file or not.
        normalize: Whether to normalize the SQL code or not.
        pad: The number of spaces to use for padding.
        indent: The number of spaces to use for indentation.
        normalize_functions: The functions to normalize.
        leading_comma: Whether to use leading commas or not.
        max_text_width: The maximum text width.
    """

    append_newline: bool = Field(default=False, exclude=True)
    normalize: bool = False
    pad: int = 2
    indent: int = 2
    normalize_functions: t.Optional[str] = None
    leading_comma: bool = False
    max_text_width: int = 80


class UIConfig(BaseConfig):
    """The UI configuration for SQLMesh.

    Args:
        format_on_save: Whether to format the SQL code on save or not.
    """

    format_on_save: bool = True
