def indent(string: str, num: int = 2) -> str:
    indentation = " " * num
    return indentation + string.replace("\n", "\n" + indentation)
