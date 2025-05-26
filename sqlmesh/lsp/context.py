from dataclasses import dataclass
from pathlib import Path
from sqlmesh.core.context import Context
import typing as t

from sqlmesh.core.model.definition import SqlModel
from sqlmesh.lsp.custom import RenderModelEntry
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
        model_map: t.Dict[Path, ModelTarget] = {}
        for model in context.models.values():
            if model._path is not None:
                uri = model._path
                if uri in model_map:
                    model_map[uri].names.append(model.name)
                else:
                    model_map[uri] = ModelTarget(names=[model.name])

        # Add standalone audits to the map
        audit_map: t.Dict[Path, AuditTarget] = {}
        for audit in context.standalone_audits.values():
            if audit._path is not None:
                uri = audit._path
                if uri not in audit_map:
                    audit_map[uri] = AuditTarget(name=audit.name)

        self.map: t.Dict[Path, t.Union[ModelTarget, AuditTarget]] = {
            **model_map,
            **audit_map,
        }


def render_model(context: LSPContext, uri: URI) -> t.Iterator[RenderModelEntry]:
    target = context.map[uri.to_path()]
    if isinstance(target, AuditTarget):
        audit = context.context.standalone_audits[target.name]
        definition = audit.render_definition(
            include_python=False,
            render_query=True,
        )
        rendered_query = [render.sql(dialect=audit.dialect, pretty=True) for render in definition]
        yield RenderModelEntry(
            name=audit.name,
            fqn=audit.fqn,
            description=audit.description,
            rendered_query="\n\n".join(rendered_query),
        )
    if isinstance(target, ModelTarget):
        for name in target.names:
            model = context.context.get_model(name)
            if isinstance(model, SqlModel):
                rendered_query = [
                    render.sql(dialect=model.dialect, pretty=True)
                    for render in model.render_definition(
                        include_python=False,
                        render_query=True,
                    )
                ]
                yield RenderModelEntry(
                    name=model.name,
                    fqn=model.fqn,
                    description=model.description,
                    rendered_query="\n\n".join(rendered_query),
                )
