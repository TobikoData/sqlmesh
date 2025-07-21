from dataclasses import dataclass
from pathlib import Path
from sqlmesh.core.context import Context
import typing as t

from sqlmesh.core.model.definition import SqlModel
from sqlmesh.core.linter.definition import AnnotatedRuleViolation
from sqlmesh.lsp.custom import ModelForRendering
from sqlmesh.lsp.custom import AllModelsResponse, RenderModelEntry
from sqlmesh.lsp.uri import URI
from lsprotocol import types


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

    def get_code_actions(
        self, uri: URI, params: types.CodeActionParams
    ) -> t.Optional[t.List[t.Union[types.Command, types.CodeAction]]]:
        """Get code actions for a file."""

        # Get the violations (which contain the fixes)
        violations = self.lint_model(uri)

        # Convert violations to a map for quick lookup
        # Use a hashable representation of Range as the key
        violation_map: t.Dict[
            t.Tuple[str, t.Tuple[int, int, int, int]], AnnotatedRuleViolation
        ] = {}
        for violation in violations:
            if violation.violation_range:
                lsp_diagnostic = self.diagnostic_to_lsp_diagnostic(violation)
                if lsp_diagnostic:
                    # Create a hashable key from the diagnostic message and range
                    key = (
                        lsp_diagnostic.message,
                        (
                            lsp_diagnostic.range.start.line,
                            lsp_diagnostic.range.start.character,
                            lsp_diagnostic.range.end.line,
                            lsp_diagnostic.range.end.character,
                        ),
                    )
                    violation_map[key] = violation

        # Get diagnostics in the requested range
        diagnostics = params.context.diagnostics if params.context else []

        code_actions: t.List[t.Union[types.Command, types.CodeAction]] = []

        for diagnostic in diagnostics:
            # Find the corresponding violation
            key = (
                diagnostic.message,
                (
                    diagnostic.range.start.line,
                    diagnostic.range.start.character,
                    diagnostic.range.end.line,
                    diagnostic.range.end.character,
                ),
            )
            found_violation = violation_map.get(key)

            if found_violation is not None and found_violation.fixes:
                # Create code actions for each fix
                for fix in found_violation.fixes:
                    # Convert our Fix to LSP TextEdits
                    text_edits = []
                    for edit in fix.edits:
                        text_edits.append(
                            types.TextEdit(
                                range=types.Range(
                                    start=types.Position(
                                        line=edit.range.start.line,
                                        character=edit.range.start.character,
                                    ),
                                    end=types.Position(
                                        line=edit.range.end.line,
                                        character=edit.range.end.character,
                                    ),
                                ),
                                new_text=edit.new_text,
                            )
                        )

                    # Create the code action
                    code_action = types.CodeAction(
                        title=fix.title,
                        kind=types.CodeActionKind.QuickFix,
                        diagnostics=[diagnostic],
                        edit=types.WorkspaceEdit(changes={params.text_document.uri: text_edits}),
                    )
                    code_actions.append(code_action)

        return code_actions if code_actions else None

    def list_of_models_for_rendering(self) -> t.List[ModelForRendering]:
        """Get a list of models for rendering.

        Returns:
            List of ModelForRendering objects.
        """
        return [
            ModelForRendering(
                name=model.name,
                fqn=model.fqn,
                description=model.description,
                uri=URI.from_path(model._path).value,
            )
            for model in self.context.models.values()
            if isinstance(model, SqlModel) and model._path is not None
        ] + [
            ModelForRendering(
                name=audit.name,
                fqn=audit.fqn,
                description=audit.description,
                uri=URI.from_path(audit._path).value,
            )
            for audit in self.context.standalone_audits.values()
            if audit._path is not None
        ]

    @staticmethod
    def get_completions(
        self: t.Optional["LSPContext"] = None,
        uri: t.Optional[URI] = None,
        file_content: t.Optional[str] = None,
    ) -> AllModelsResponse:
        """Get completion suggestions for a file"""
        from sqlmesh.lsp.completions import get_sql_completions

        return get_sql_completions(self, uri, file_content)

    @staticmethod
    def diagnostics_to_lsp_diagnostics(
        diagnostics: t.List[AnnotatedRuleViolation],
    ) -> t.List[types.Diagnostic]:
        """
        Converts a list of AnnotatedRuleViolations to a list of LSP diagnostics. It will remove duplicates based on the message and range.
        """
        lsp_diagnostics = {}
        for diagnostic in diagnostics:
            lsp_diagnostic = LSPContext.diagnostic_to_lsp_diagnostic(diagnostic)
            if lsp_diagnostic is not None:
                # Create a unique key combining message and range
                diagnostic_key = (
                    lsp_diagnostic.message,
                    lsp_diagnostic.range.start.line,
                    lsp_diagnostic.range.start.character,
                    lsp_diagnostic.range.end.line,
                    lsp_diagnostic.range.end.character,
                )
                if diagnostic_key not in lsp_diagnostics:
                    lsp_diagnostics[diagnostic_key] = lsp_diagnostic
        return list(lsp_diagnostics.values())

    @staticmethod
    def diagnostic_to_lsp_diagnostic(
        diagnostic: AnnotatedRuleViolation,
    ) -> t.Optional[types.Diagnostic]:
        if diagnostic.model._path is None:
            return None
        if not diagnostic.violation_range:
            with open(diagnostic.model._path, "r", encoding="utf-8") as file:
                lines = file.readlines()
            diagnostic_range = types.Range(
                start=types.Position(line=0, character=0),
                end=types.Position(line=len(lines) - 1, character=len(lines[-1])),
            )
        else:
            diagnostic_range = types.Range(
                start=types.Position(
                    line=diagnostic.violation_range.start.line,
                    character=diagnostic.violation_range.start.character,
                ),
                end=types.Position(
                    line=diagnostic.violation_range.end.line,
                    character=diagnostic.violation_range.end.character,
                ),
            )

        # Get rule definition location for diagnostics link
        rule_location = diagnostic.rule.get_definition_location()
        rule_uri_wihout_extension = URI.from_path(rule_location.file_path)
        rule_uri = f"{rule_uri_wihout_extension.value}#L{rule_location.start_line}"

        # Use URI format to create a link for "related information"
        return types.Diagnostic(
            range=diagnostic_range,
            message=diagnostic.violation_msg,
            severity=types.DiagnosticSeverity.Error
            if diagnostic.violation_type == "error"
            else types.DiagnosticSeverity.Warning,
            source="sqlmesh",
            code=diagnostic.rule.name,
            code_description=types.CodeDescription(href=rule_uri),
        )
