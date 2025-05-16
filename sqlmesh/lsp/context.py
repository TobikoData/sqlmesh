from dataclasses import dataclass
from sqlmesh.core.context import Context
import typing as t

from sqlmesh.lsp.uri import URI


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
        model_map: t.Dict[URI, ModelTarget] = {}
        for model in context.models.values():
            if model._path is not None:
                uri = URI.from_path(model._path)
                if uri in model_map:
                    model_map[uri].names.append(model.name)
                else:
                    model_map[uri] = ModelTarget(names=[model.name])

        # Add standalone audits to the map
        audit_map: t.Dict[URI, AuditTarget] = {}
        for audit in context.standalone_audits.values():
            if audit._path is not None:
                uri = URI.from_path(audit._path)
                if uri not in audit_map:
                    audit_map[uri] = AuditTarget(name=audit.name)

        self.map: t.Dict[URI, t.Union[ModelTarget, AuditTarget]] = {
            **model_map,
            **audit_map,
        }
