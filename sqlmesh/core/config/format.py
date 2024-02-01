from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig


class UIFormatterConfig(BaseConfig):
    """The UI formatter configuration for SQLMesh.

    Args:
        enabled: Whether to enable the UI formatter or not.
    """

    enabled: bool = True


class FormatConfig(BaseConfig):
    """The format configuration for SQL code.

    Args:
        append_newline: Whether to append a newline to the end of the file or not.
        normalize: Whether to normalize the SQL code or not.
        pad: The number of spaces to use for padding.
        indent: The number of spaces to use for indentation.
        normalize_functions: The functions to normalize.
        leading_comma: Whether to use leading commas or not.
        max_text_width: The maximum text width.
        ui: The UI formatter configuration.
    """

    append_newline: bool = False
    normalize: bool = False
    pad: int = 2
    indent: int = 2
    normalize_functions: t.Optional[str] = None
    leading_comma: bool = False
    max_text_width: int = 80
    ui: UIFormatterConfig = UIFormatterConfig()

    @property
    def generator_options(self) -> t.Dict[str, t.Any]:
        """The generator options for the SQL code formatter.

        Returns:
            The generator options.
        """
        return {
            "normalize": self.normalize,
            "pad": self.pad,
            "indent": self.indent,
            "normalize_functions": self.normalize_functions,
            "leading_comma": self.leading_comma,
            "max_text_width": self.max_text_width,
        }
