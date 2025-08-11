from sqlmesh.core.console import TerminalConsole


class DbtCliConsole(TerminalConsole):
    # TODO: build this out

    def print(self, msg: str) -> None:
        return self._print(msg)
