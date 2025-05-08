from dataclasses import dataclass
from pathlib import Path
from sqlmesh.core.context import Context
import typing as t


@dataclass
class ModelTarget:
    """Information about models in a file."""

    names: t.List[str]


@dataclass
class AuditTarget:
    """Information about standalone audits in a file."""

    name: str


class LSPContext:
    """
    A context that is used for linting. It contains the context and a reverse map of file uri to
    model names and standalone audit names.
    """

    def __init__(self, context: Context) -> None:
        self.context = context

        # Add models to the map
        model_map: t.Dict[str, ModelTarget] = {}
        for model in context.models.values():
            if model._path is not None:
                path = Path(model._path).resolve()
                uri = f"file://{path.as_posix()}"
                if uri in model_map:
                    model_map[uri].names.append(model.name)
                else:
                    model_map[uri] = ModelTarget(names=[model.name])

        # Add standalone audits to the map
        audit_map: t.Dict[str, AuditTarget] = {}
        for audit in context.standalone_audits.values():
            if audit._path is not None:
                path = Path(audit._path).resolve()
                uri = f"file://{path.as_posix()}"
                if uri not in audit_map:
                    audit_map[uri] = AuditTarget(name=audit.name)

        self.map: t.Dict[str, t.Union[ModelTarget, AuditTarget]] = {
            **model_map,
            **audit_map,
        }
