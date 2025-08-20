from pathlib import Path

from sqlmesh.core.linter.rule import Range, Position
from sqlmesh.utils.pydantic import PydanticModel
from sqlglot import tokenize, TokenType, Token
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


def read_range_from_string(content: str, text_range: Range) -> str:
    lines = content.splitlines(keepends=False)

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

    return read_range_from_string("".join(lines), text_range)


def get_start_and_end_of_model_block(
    tokens: t.List[Token],
) -> t.Optional[t.Tuple[int, int]]:
    """
    Returns the start and end tokens of the MODEL block in an SQL file.
    The MODEL block is defined as the first occurrence of the keyword "MODEL" followed by
    an opening parenthesis and a closing parenthesis that matches the opening one.
    """
    # 1) Find the MODEL token
    try:
        model_idx = next(
            i
            for i, tok in enumerate(tokens)
            if tok.token_type is TokenType.VAR and tok.text.upper() == "MODEL"
        )
    except StopIteration:
        return None

    # 2) Find the opening parenthesis for the MODEL properties list
    try:
        lparen_idx = next(
            i
            for i in range(model_idx + 1, len(tokens))
            if tokens[i].token_type is TokenType.L_PAREN
        )
    except StopIteration:
        return None

    # 3) Find the matching closing parenthesis by looking for the first semicolon after
    # the opening parenthesis and assuming the MODEL block ends there.
    try:
        closing_semicolon = next(
            i
            for i in range(lparen_idx + 1, len(tokens))
            if tokens[i].token_type is TokenType.SEMICOLON
        )
        # If we find a semicolon, we can assume the MODEL block ends there
        rparen_idx = closing_semicolon - 1
        if tokens[rparen_idx].token_type is TokenType.R_PAREN:
            return (lparen_idx, rparen_idx)
        return None
    except StopIteration:
        return None


def get_range_of_model_block(
    sql: str,
    dialect: str,
) -> t.Optional[Range]:
    """
    Get the range of the model block in an SQL file,
    """
    tokens = tokenize(sql, dialect=dialect)
    block = get_start_and_end_of_model_block(tokens)
    if not block:
        return None
    (start_idx, end_idx) = block
    start = tokens[start_idx - 1]
    end = tokens[end_idx + 1]
    start_position = TokenPositionDetails(
        line=start.line,
        col=start.col,
        start=start.start,
        end=start.end,
    )
    end_position = TokenPositionDetails(
        line=end.line,
        col=end.col,
        start=end.start,
        end=end.end,
    )
    splitlines = sql.splitlines()
    return Range(
        start=start_position.to_range(splitlines).start,
        end=end_position.to_range(splitlines).end,
    )


def get_range_of_a_key_in_model_block(
    sql: str,
    dialect: str,
    key: str,
) -> t.Optional[t.Tuple[Range, Range]]:
    """
    Get the ranges of a specific key and its value in the MODEL block of an SQL file.

    Returns a tuple of (key_range, value_range) if found, otherwise None.
    """
    tokens = tokenize(sql, dialect=dialect)
    if not tokens:
        return None

    block = get_start_and_end_of_model_block(tokens)
    if not block:
        return None
    (lparen_idx, rparen_idx) = block

    # 4) Scan within the MODEL property list for the key at top-level (depth == 1)
    # Initialize depth to 1 since we're inside the first parentheses
    depth = 1
    for i in range(lparen_idx + 1, rparen_idx):
        tok = tokens[i]
        tt = tok.token_type

        if tt is TokenType.L_PAREN:
            depth += 1
            continue
        if tt is TokenType.R_PAREN:
            depth -= 1
            # If we somehow exit before rparen_idx, stop early
            if depth <= 0:
                break
            continue

        if depth == 1 and tt is TokenType.VAR and tok.text.upper() == key.upper():
            # Validate key position: it should immediately follow '(' or ',' at top level
            prev_idx = i - 1
            prev_tt = tokens[prev_idx].token_type if prev_idx >= 0 else None
            if prev_tt not in (TokenType.L_PAREN, TokenType.COMMA):
                continue

            # Key range
            lines = sql.splitlines()
            key_start = TokenPositionDetails(
                line=tok.line, col=tok.col, start=tok.start, end=tok.end
            )
            key_range = key_start.to_range(lines)

            value_start_idx = i + 1
            if value_start_idx >= rparen_idx:
                return None

            # Walk to the end of the value expression: until top-level comma or closing paren
            # Track internal nesting for (), [], {}
            nested = 0
            j = value_start_idx
            value_end_idx = value_start_idx

            def is_open(t: TokenType) -> bool:
                return t in (TokenType.L_PAREN, TokenType.L_BRACE, TokenType.L_BRACKET)

            def is_close(t: TokenType) -> bool:
                return t in (TokenType.R_PAREN, TokenType.R_BRACE, TokenType.R_BRACKET)

            while j < rparen_idx:
                ttype = tokens[j].token_type
                if is_open(ttype):
                    nested += 1
                elif is_close(ttype):
                    nested -= 1

                # End of value: at top-level (nested == 0) encountering a comma or the end paren
                if nested == 0 and (
                    ttype is TokenType.COMMA or (ttype is TokenType.R_PAREN and depth == 1)
                ):
                    # For comma, don't include it in the value range
                    # For closing paren, include it only if it's part of the value structure
                    if ttype is TokenType.COMMA:
                        # Don't include the comma in the value range
                        break
                    else:
                        # Include the closing parenthesis in the value range
                        value_end_idx = j
                        break

                value_end_idx = j
                j += 1

            value_start_tok = tokens[value_start_idx]
            value_end_tok = tokens[value_end_idx]

            value_start_pos = TokenPositionDetails(
                line=value_start_tok.line,
                col=value_start_tok.col,
                start=value_start_tok.start,
                end=value_start_tok.end,
            )
            value_end_pos = TokenPositionDetails(
                line=value_end_tok.line,
                col=value_end_tok.col,
                start=value_end_tok.start,
                end=value_end_tok.end,
            )
            value_range = Range(
                start=value_start_pos.to_range(lines).start,
                end=value_end_pos.to_range(lines).end,
            )

            return (key_range, value_range)

    return None
