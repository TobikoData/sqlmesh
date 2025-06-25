from pathlib import Path

from sqlmesh.core.linter.rule import Position, Range
from sqlmesh.utils.pydantic import PydanticModel
import typing as t


class TokenPositionDetails(PydanticModel):
    """
    Details about a token's position in the source code in the structure provided by SQLGlot.

    Attributes:
        line (int): The line that the token ends on.
        col (int): The column that the token ends on.
        start (int): The start index of the token.
        end (int): The ending index of the token.
    """

    line: int
    col: int
    start: int
    end: int

    @staticmethod
    def from_meta(meta: t.Dict[str, int]) -> "TokenPositionDetails":
        return TokenPositionDetails(
            line=meta["line"],
            col=meta["col"],
            start=meta["start"],
            end=meta["end"],
        )

    def to_range(self, read_file: t.Optional[t.List[str]]) -> Range:
        """
        Convert a TokenPositionDetails object to a Range object.

        In the circumstances where the token's start and end positions are the same,
        there is no need for a read_file parameter, as the range can be derived from the token's
        line and column. This is an optimization to avoid unnecessary file reads and should
        only be used when the token represents a single character or position in the file.

        If the token's start and end positions are different, the read_file parameter is required.

        :param read_file: List of lines from the file. Optional
        :return: A Range object representing the token's position
        """
        if self.start == self.end:
            # If the start and end positions are the same, we can create a range directly
            return Range(
                start=Position(line=self.line - 1, character=self.col - 1),
                end=Position(line=self.line - 1, character=self.col),
            )

        if read_file is None:
            raise ValueError("read_file must be provided when start and end positions differ.")

        # Convert from 1-indexed to 0-indexed for line only
        end_line_0 = self.line - 1
        end_col_0 = self.col

        # Find the start line and column by counting backwards from the end position
        start_pos = self.start
        end_pos = self.end

        # Initialize with the end position
        start_line_0 = end_line_0
        start_col_0 = end_col_0 - (end_pos - start_pos + 1)

        # If start_col_0 is negative, we need to go back to previous lines
        while start_col_0 < 0 and start_line_0 > 0:
            start_line_0 -= 1
            start_col_0 += len(read_file[start_line_0])
            # Account for newline character
            if start_col_0 >= 0:
                break
            start_col_0 += 1  # For the newline character

        # Ensure we don't have negative values
        start_col_0 = max(0, start_col_0)
        return Range(
            start=Position(line=start_line_0, character=start_col_0),
            end=Position(line=end_line_0, character=end_col_0),
        )


def read_range_from_file(file: Path, text_range: Range) -> str:
    """
    Read the file and return the content within the specified range.

    Args:
        file: Path to the file to read
        text_range: The range of text to extract

    Returns:
        The content within the specified range
    """
    with file.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    # Ensure the range is within bounds
    start_line = max(0, text_range.start.line)
    end_line = min(len(lines), text_range.end.line + 1)

    if start_line >= end_line:
        return ""

    # Extract the relevant portions of each line
    result = []
    for i in range(start_line, end_line):
        line = lines[i]
        start_char = text_range.start.character if i == text_range.start.line else 0
        end_char = text_range.end.character if i == text_range.end.line else len(line)
        result.append(line[start_char:end_char])

    return "".join(result)
