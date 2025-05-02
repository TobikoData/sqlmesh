from collections import defaultdict
from pathlib import Path
from sqlmesh.core.context import Context
import typing as t


class LSPContext:
    """
    A context that is used for linting. It contains the context and a reverse map of file uri to model names .
    """

    def __init__(self, context: Context) -> None:
        self.context = context
        map: t.Dict[str, t.List[str]] = defaultdict(list)
        for model in context.models.values():
            if model._path is not None:
                path = Path(model._path).resolve()
                map[f"file://{path.as_posix()}"].append(model.name)
        self.map = map
