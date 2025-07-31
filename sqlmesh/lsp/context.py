from dataclasses import dataclass
from pathlib import Path
from pygls.server import LanguageServer
from sqlmesh.core.context import Context
import typing as t
from sqlmesh.core.linter.rule import Range
from sqlmesh.core.model.definition import SqlModel, ExternalModel
from sqlmesh.core.linter.definition import AnnotatedRuleViolation
from sqlmesh.core.schema_loader import get_columns
from sqlmesh.lsp.commands import EXTERNAL_MODEL_UPDATE_COLUMNS
from sqlmesh.lsp.custom import ModelForRendering, TestEntry, RunTestResponse
from sqlmesh.lsp.custom import AllModelsResponse, RenderModelEntry
from sqlmesh.lsp.tests_ranges import get_test_ranges
from sqlmesh.lsp.helpers import to_lsp_range
from sqlmesh.lsp.uri import URI
from lsprotocol import types
from sqlmesh.utils import yaml
from sqlmesh.utils.lineage import get_yaml_model_name_ranges


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

    def list_workspace_tests(self) -> t.List[TestEntry]:
        """List all tests in the workspace."""
        tests = self.context.load_model_tests()

        # Use a set to ensure unique URIs
        unique_test_uris = {URI.from_path(test.path).value for test in tests}
        test_uris: t.Dict[str, t.Dict[str, Range]] = {}
        for uri in unique_test_uris:
            test_ranges = get_test_ranges(URI(uri).to_path())
            if uri not in test_uris:
                test_uris[uri] = {}
            test_uris[uri].update(test_ranges)
        return [
            TestEntry(
                name=test.test_name,
                uri=URI.from_path(test.path).value,
                range=test_uris.get(URI.from_path(test.path).value, {}).get(test.test_name),
            )
            for test in tests
        ]

    def get_document_tests(self, uri: URI) -> t.List[TestEntry]:
        """Get tests for a specific document.

        Args:
            uri: The URI of the file to get tests for.

        Returns:
            List of TestEntry objects for the specified document.
        """
        tests = self.context.load_model_tests(tests=[str(uri.to_path())])
        test_ranges = get_test_ranges(uri.to_path())
        return [
            TestEntry(
                name=test.test_name,
                uri=URI.from_path(test.path).value,
                range=test_ranges.get(test.test_name),
            )
            for test in tests
        ]

    def run_test(self, uri: URI, test_name: str) -> RunTestResponse:
        """Run a specific test for a model.

        Args:
            uri: The URI of the file containing the test.
            test_name: The name of the test to run.

        Returns:
            List of annotated rule violations from the test run.
        """
        path = uri.to_path()
        results = self.context.test(
            tests=[str(path)],
            match_patterns=[test_name],
        )
        if results.testsRun != 1:
            raise ValueError(f"Expected to run 1 test, but ran {results.testsRun} tests.")
        if len(results.successes) == 1:
            return RunTestResponse(success=True)
        return RunTestResponse(
            success=False,
            error_message=str(results.failures[0][1]),
        )

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
                    changes: t.Dict[str, t.List[types.TextEdit]] = {}
                    document_changes: t.List[
                        t.Union[
                            types.TextDocumentEdit,
                            types.CreateFile,
                            types.RenameFile,
                            types.DeleteFile,
                        ]
                    ] = []

                    for create in fix.create_files:
                        create_uri = URI.from_path(create.path).value
                        document_changes.append(types.CreateFile(uri=create_uri))
                        document_changes.append(
                            types.TextDocumentEdit(
                                text_document=types.OptionalVersionedTextDocumentIdentifier(
                                    uri=create_uri,
                                    version=None,
                                ),
                                edits=[
                                    types.TextEdit(
                                        range=types.Range(
                                            start=types.Position(line=0, character=0),
                                            end=types.Position(line=0, character=0),
                                        ),
                                        new_text=create.text,
                                    )
                                ],
                            )
                        )

                    for edit in fix.edits:
                        uri_key = URI.from_path(edit.path).value
                        if uri_key not in changes:
                            changes[uri_key] = []
                        changes[uri_key].append(
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

                    workspace_edit = types.WorkspaceEdit(
                        changes=changes if changes else None,
                        document_changes=document_changes if document_changes else None,
                    )
                    code_action = types.CodeAction(
                        title=fix.title,
                        kind=types.CodeActionKind.QuickFix,
                        diagnostics=[diagnostic],
                        edit=workspace_edit,
                    )
                    code_actions.append(code_action)

        return code_actions if code_actions else None

    def get_code_lenses(self, uri: URI) -> t.Optional[t.List[types.CodeLens]]:
        models_in_file = self.map.get(uri.to_path())
        if isinstance(models_in_file, ModelTarget):
            models = [self.context.get_model(model) for model in models_in_file.names]
            if any(isinstance(model, ExternalModel) for model in models):
                code_lenses = self._get_external_model_code_lenses(uri)
                if code_lenses:
                    return code_lenses

        return None

    def _get_external_model_code_lenses(self, uri: URI) -> t.List[types.CodeLens]:
        """Get code lenses for external models YAML files."""
        ranges = get_yaml_model_name_ranges(uri.to_path())
        if ranges is None:
            return []
        return [
            types.CodeLens(
                range=to_lsp_range(range),
                command=types.Command(
                    title="Update Columns",
                    command=EXTERNAL_MODEL_UPDATE_COLUMNS,
                    arguments=[
                        name,
                    ],
                ),
            )
            for name, range in ranges.items()
        ]

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

    def update_external_model_columns(self, ls: LanguageServer, uri: URI, model_name: str) -> bool:
        """
        Update the columns for an external model in the YAML file. Returns True if changed, False if didn't because
        of the columns already being up to date.

        In this case, the model name is the name of the external model as is defined in the YAML file, not any other version of it.

        Errors still throw exceptions to be handled by the caller.
        """
        models = yaml.load(uri.to_path())
        if not isinstance(models, list):
            raise ValueError(
                f"Expected a list of models in {uri.to_path()}, but got {type(models).__name__}"
            )

        existing_model = next((model for model in models if model.get("name") == model_name), None)
        if existing_model is None:
            raise ValueError(f"Could not find model {model_name} in {uri.to_path()}")

        existing_model_columns = existing_model.get("columns")

        # Get the adapter and fetch columns
        adapter = self.context.engine_adapter
        # Get columns for the model
        new_columns = get_columns(
            adapter=adapter,
            dialect=self.context.config.model_defaults.dialect,
            table=model_name,
            strict=True,
        )
        # Compare existing columns and matching types and if they are the same, do not update
        if existing_model_columns is not None:
            if existing_model_columns == new_columns:
                return False

        # Model index to update
        model_index = next(
            (i for i, model in enumerate(models) if model.get("name") == model_name), None
        )
        if model_index is None:
            raise ValueError(f"Could not find model {model_name} in {uri.to_path()}")

        # Get end of the file to set the edit range
        with open(uri.to_path(), "r", encoding="utf-8") as file:
            read_file = file.read()

        end_line = read_file.count("\n")
        end_character = len(read_file.splitlines()[-1]) if end_line > 0 else 0

        models[model_index]["columns"] = new_columns
        edit = types.TextDocumentEdit(
            text_document=types.OptionalVersionedTextDocumentIdentifier(
                uri=uri.value,
                version=None,
            ),
            edits=[
                types.TextEdit(
                    range=types.Range(
                        start=types.Position(line=0, character=0),
                        end=types.Position(
                            line=end_line,
                            character=end_character,
                        ),
                    ),
                    new_text=yaml.dump(models),
                )
            ],
        )
        ls.apply_edit(types.WorkspaceEdit(document_changes=[edit]))
        return True
