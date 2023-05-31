def indent(string: str, num: int = 1) -> str:
    indentation = "\t" * num
    return indentation + string.replace("\n", "\n" + indentation)
