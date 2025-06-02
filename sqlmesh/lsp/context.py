from dataclasses import dataclass
from pathlib import Path
from sqlmesh.core.context import Context
import typing as t

from sqlmesh.core.model.definition import SqlModel
from sqlmesh.core.linter.definition import AnnotatedRuleViolation
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

    map: t.Dict[Path, t.Union[ModelTarget, AuditTarget]]
    _render_cache: t.Dict[Path, t.List[RenderModelEntry]]
    _lint_cache: t.Dict[Path, t.List[AnnotatedRuleViolation]]

    def __init__(self, context: Context) -> None:
        self.context = context
        self._render_cache = {}
        self._lint_cache = {}

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

    def render_model(self, uri: URI) -> t.List[RenderModelEntry]:
        """Get rendered models for a file, using cache when available.

        Args:
            uri: The URI of the file to render.

        Returns:
            List of rendered model entries.
        """
        path = uri.to_path()

        # Check cache first
        if path in self._render_cache:
            return self._render_cache[path]

        # If not cached, render and cache
        entries: t.List[RenderModelEntry] = []
        target = self.map.get(path)

        if isinstance(target, AuditTarget):
            audit = self.context.standalone_audits[target.name]
            definition = audit.render_definition(
                include_python=False,
                render_query=True,
            )
            rendered_query = [
                render.sql(dialect=audit.dialect, pretty=True) for render in definition
            ]
            entry = RenderModelEntry(
                name=audit.name,
                fqn=audit.fqn,
                description=audit.description,
                rendered_query="\n\n".join(rendered_query),
            )
            entries.append(entry)

        elif isinstance(target, ModelTarget):
            for name in target.names:
                model = self.context.get_model(name)
                if isinstance(model, SqlModel):
                    rendered_query = [
                        render.sql(dialect=model.dialect, pretty=True)
                        for render in model.render_definition(
                            include_python=False,
                            render_query=True,
                        )
                    ]
                    entry = RenderModelEntry(
                        name=model.name,
                        fqn=model.fqn,
                        description=model.description,
                        rendered_query="\n\n".join(rendered_query),
                    )
                    entries.append(entry)

        # Store in cache
        self._render_cache[path] = entries
        return entries

    def lint_model(self, uri: URI) -> t.List[AnnotatedRuleViolation]:
        """Get lint diagnostics for a model, using cache when available.

        Args:
            uri: The URI of the file to lint.

        Returns:
            List of annotated rule violations.
        """
        path = uri.to_path()

        # Check cache first
        if path in self._lint_cache:
            return self._lint_cache[path]

        # If not cached, lint and cache
        target = self.map.get(path)
        if target is None or not isinstance(target, ModelTarget):
            return []

        diagnostics = self.context.lint_models(
            target.names,
            raise_on_error=False,
        )

        # Store in cache
        self._lint_cache[path] = diagnostics
        return diagnostics
