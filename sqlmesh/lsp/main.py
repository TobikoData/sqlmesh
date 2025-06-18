#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration, refactored without globals."""

from itertools import chain
import logging
import typing as t
from pathlib import Path
import urllib.parse

from lsprotocol import types
from pygls.server import LanguageServer

from sqlmesh._version import __version__
from sqlmesh.core.context import Context
from sqlmesh.core.linter.definition import AnnotatedRuleViolation
from sqlmesh.lsp.api import (
    API_FEATURE,
    ApiRequest,
    ApiResponseGetColumnLineage,
    ApiResponseGetLineage,
    ApiResponseGetModels,
)
from sqlmesh.lsp.completions import get_sql_completions
from sqlmesh.lsp.context import (
    LSPContext,
    ModelTarget,
)
from sqlmesh.lsp.custom import (
    ALL_MODELS_FEATURE,
    ALL_MODELS_FOR_RENDER_FEATURE,
    RENDER_MODEL_FEATURE,
    SUPPORTED_METHODS_FEATURE,
    FORMAT_PROJECT_FEATURE,
    AllModelsRequest,
    AllModelsResponse,
    AllModelsForRenderRequest,
    AllModelsForRenderResponse,
    CustomMethodResponseBaseClass,
    RenderModelRequest,
    RenderModelResponse,
    SupportedMethodsRequest,
    SupportedMethodsResponse,
    FormatProjectRequest,
    FormatProjectResponse,
    CustomMethod,
)
from sqlmesh.lsp.hints import get_hints
from sqlmesh.lsp.reference import (
    LSPCteReference,
    LSPModelReference,
    LSPExternalModelReference,
    get_references,
    get_all_references,
)
from sqlmesh.lsp.uri import URI
from web.server.api.endpoints.lineage import column_lineage, model_lineage
from web.server.api.endpoints.models import get_models


class SQLMeshLanguageServer:
    def __init__(
        self,
        context_class: t.Type[Context],
        server_name: str = "sqlmesh_lsp",
        version: str = __version__,
    ):
        """
        :param context_class: A class that inherits from `Context`.
        :param server_name: Name for the language server.
        :param version: Version string.
        """
        self.server = LanguageServer(server_name, version)
        self.context_class = context_class
        self.lsp_context: t.Optional[LSPContext] = None
        self.workspace_folders: t.List[Path] = []

        self.has_raised_loading_error: bool = False

        self.client_supports_pull_diagnostics = False
        self._supported_custom_methods: t.Dict[
            str,
            t.Callable[
                # mypy unable to recognise the base class
                [LanguageServer, t.Any],
                t.Any,
            ],
        ] = {
            ALL_MODELS_FEATURE: self._custom_all_models,
            RENDER_MODEL_FEATURE: self._custom_render_model,
            ALL_MODELS_FOR_RENDER_FEATURE: self._custom_all_models_for_render,
            API_FEATURE: self._custom_api,
            SUPPORTED_METHODS_FEATURE: self._custom_supported_methods,
            FORMAT_PROJECT_FEATURE: self._custom_format_project,
        }

        # Register LSP features (e.g., formatting, hover, etc.)
        self._register_features()

    # All the custom LSP methods are registered here and prefixed with _custom
    def _custom_all_models(self, ls: LanguageServer, params: AllModelsRequest) -> AllModelsResponse:
        uri = URI(params.textDocument.uri)
        # Get the document content
        content = None
        try:
            document = ls.workspace.get_text_document(params.textDocument.uri)
            content = document.source
        except Exception:
            pass
        try:
            context = self._context_get_or_load(uri)
            return LSPContext.get_completions(context, uri, content)
        except Exception as e:
            from sqlmesh.lsp.completions import get_sql_completions

            return get_sql_completions(None, URI(params.textDocument.uri), content)

    def _custom_render_model(
        self, ls: LanguageServer, params: RenderModelRequest
    ) -> RenderModelResponse:
        uri = URI(params.textDocumentUri)
        context = self._context_get_or_load(uri)
        return RenderModelResponse(models=context.render_model(uri))

    def _custom_all_models_for_render(
        self, ls: LanguageServer, params: AllModelsForRenderRequest
    ) -> AllModelsForRenderResponse:
        context = self._context_get_or_load()
        return AllModelsForRenderResponse(models=context.list_of_models_for_rendering())

    def _custom_format_project(
        self, ls: LanguageServer, params: FormatProjectRequest
    ) -> FormatProjectResponse:
        """Format all models in the current project."""
        try:
            context = self._context_get_or_load()
            context.context.format()
            return FormatProjectResponse()
        except Exception as e:
            ls.log_trace(f"Error formatting project: {e}")
            return FormatProjectResponse()

    def _custom_api(
        self, ls: LanguageServer, request: ApiRequest
    ) -> t.Union[ApiResponseGetModels, ApiResponseGetColumnLineage, ApiResponseGetLineage]:
        ls.log_trace(f"API request: {request}")
        context = self._context_get_or_load()

        parsed_url = urllib.parse.urlparse(request.url)
        path_parts = parsed_url.path.strip("/").split("/")

        if request.method == "GET":
            if path_parts == ["api", "models"]:
                # /api/models
                return ApiResponseGetModels(data=get_models(context.context))

            if path_parts[:2] == ["api", "lineage"]:
                if len(path_parts) == 3:
                    # /api/lineage/{model}
                    model_name = urllib.parse.unquote(path_parts[2])
                    lineage = model_lineage(model_name, context.context)
                    non_set_lineage = {k: v for k, v in lineage.items() if v is not None}
                    return ApiResponseGetLineage(data=non_set_lineage)

                if len(path_parts) == 4:
                    # /api/lineage/{model}/{column}
                    model_name = urllib.parse.unquote(path_parts[2])
                    column = urllib.parse.unquote(path_parts[3])
                    models_only = False
                    if hasattr(request, "params"):
                        models_only = bool(getattr(request.params, "models_only", False))
                    column_lineage_response = column_lineage(
                        model_name, column, models_only, context.context
                    )
                    return ApiResponseGetColumnLineage(data=column_lineage_response)

        raise NotImplementedError(f"API request not implemented: {request.url}")

    def _custom_supported_methods(
        self, ls: LanguageServer, params: SupportedMethodsRequest
    ) -> SupportedMethodsResponse:
        """Return all supported custom LSP methods."""
        return SupportedMethodsResponse(
            methods=[
                CustomMethod(
                    name=name,
                )
                for name in self._supported_custom_methods
            ]
        )

    def _register_features(self) -> None:
        """Register LSP features on the internal LanguageServer instance."""
        for name, method in self._supported_custom_methods.items():

            def create_function_call(method_func: t.Callable) -> t.Callable:
                def function_call(ls: LanguageServer, params: t.Any) -> t.Dict[str, t.Any]:
                    try:
                        response = method_func(ls, params)
                    except Exception as e:
                        response = CustomMethodResponseBaseClass(response_error=str(e))
                    return response.model_dump(mode="json")

                return function_call

            self.server.feature(name)(create_function_call(method))

        @self.server.feature(types.INITIALIZE)
        def initialize(ls: LanguageServer, params: types.InitializeParams) -> None:
            """Initialize the server when the client connects."""
            try:
                # Check if the client supports pull diagnostics
                if params.capabilities and params.capabilities.text_document:
                    diagnostics = getattr(params.capabilities.text_document, "diagnostic", None)
                    if diagnostics:
                        self.client_supports_pull_diagnostics = True
                        ls.log_trace("Client supports pull diagnostics")
                    else:
                        self.client_supports_pull_diagnostics = False
                        ls.log_trace("Client does not support pull diagnostics")
                else:
                    self.client_supports_pull_diagnostics = False

                if params.workspace_folders:
                    # Store all workspace folders for later use
                    self.workspace_folders = [
                        Path(self._uri_to_path(folder.uri)) for folder in params.workspace_folders
                    ]

                    # Try to find a SQLMesh config file in any workspace folder (only at the root level)
                    for folder_path in self.workspace_folders:
                        # Only check for config files directly in the workspace directory
                        for ext in ("py", "yml", "yaml"):
                            config_path = folder_path / f"config.{ext}"
                            if config_path.exists():
                                if self._create_lsp_context([folder_path]):
                                    loaded_sqlmesh_message(ls, folder_path)
                                    return  # Exit after successfully loading any config
            except Exception as e:
                ls.log_trace(
                    f"Error initializing SQLMesh context: {e}",
                )

        @self.server.feature(types.TEXT_DOCUMENT_DID_OPEN)
        def did_open(ls: LanguageServer, params: types.DidOpenTextDocumentParams) -> None:
            uri = URI(params.text_document.uri)
            context = self._context_get_or_load(uri)

            # Only publish diagnostics if client doesn't support pull diagnostics
            if not self.client_supports_pull_diagnostics:
                diagnostics = context.lint_model(uri)
                ls.publish_diagnostics(
                    params.text_document.uri,
                    SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(diagnostics),
                )

        @self.server.feature(types.TEXT_DOCUMENT_DID_SAVE)
        def did_save(ls: LanguageServer, params: types.DidSaveTextDocumentParams) -> None:
            uri = URI(params.text_document.uri)
            if self.lsp_context is None:
                return

            context = self.lsp_context.context
            context.load()
            self.lsp_context = LSPContext(context)

            # Only publish diagnostics if client doesn't support pull diagnostics
            if not self.client_supports_pull_diagnostics:
                diagnostics = self.lsp_context.lint_model(uri)
                ls.publish_diagnostics(
                    params.text_document.uri,
                    SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(diagnostics),
                )

        @self.server.feature(types.TEXT_DOCUMENT_FORMATTING)
        def formatting(
            ls: LanguageServer, params: types.DocumentFormattingParams
        ) -> t.List[types.TextEdit]:
            """Format the document using SQLMesh `format_model_expressions`."""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)
                document = ls.workspace.get_text_document(params.text_document.uri)
                before = document.source

                target = next(
                    (
                        target
                        for target in chain(
                            context.context._models.values(),
                            context.context._audits.values(),
                        )
                        if target._path is not None
                        and target._path.suffix == ".sql"
                        and (target._path.samefile(uri.to_path()))
                    ),
                    None,
                )
                if target is None:
                    return []
                after = context.context._format(
                    target=target,
                    before=before,
                )
                return [
                    types.TextEdit(
                        range=types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(
                                line=len(document.lines),
                                character=len(document.lines[-1]) if document.lines else 0,
                            ),
                        ),
                        new_text=after,
                    )
                ]
            except Exception as e:
                ls.show_message(f"Error formatting SQL: {e}", types.MessageType.Error)
                return []

        @self.server.feature(types.TEXT_DOCUMENT_HOVER)
        def hover(ls: LanguageServer, params: types.HoverParams) -> t.Optional[types.Hover]:
            """Provide hover information for an object."""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)
                document = ls.workspace.get_text_document(params.text_document.uri)

                references = get_references(context, uri, params.position)
                if not references:
                    return None
                reference = references[0]
                if isinstance(reference, LSPCteReference) or not reference.markdown_description:
                    return None
                return types.Hover(
                    contents=types.MarkupContent(
                        kind=types.MarkupKind.Markdown,
                        value=reference.markdown_description,
                    ),
                    range=reference.range,
                )

            except Exception as e:
                ls.log_trace(
                    f"Error getting hover information: {e}",
                )
                return None

        @self.server.feature(types.TEXT_DOCUMENT_INLAY_HINT)
        def inlay_hint(
            ls: LanguageServer, params: types.InlayHintParams
        ) -> t.List[types.InlayHint]:
            """Implement type hints for sql columns as inlay hints"""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)

                start_line = params.range.start.line
                end_line = params.range.end.line
                hints = get_hints(context, uri, start_line, end_line)
                return hints

            except Exception as e:
                return []

        @self.server.feature(types.TEXT_DOCUMENT_DEFINITION)
        def goto_definition(
            ls: LanguageServer, params: types.DefinitionParams
        ) -> t.List[types.LocationLink]:
            """Jump to an object's definition."""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)

                references = get_references(context, uri, params.position)
                location_links = []
                for reference in references:
                    # Use target_range if available (CTEs, Macros, and external models in YAML)
                    if isinstance(reference, LSPModelReference):
                        # Regular SQL models - default to start of file
                        target_range = types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        )
                        target_selection_range = types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        )
                    elif isinstance(reference, LSPExternalModelReference):
                        # External models may have target_range set for YAML files
                        target_range = types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        )
                        target_selection_range = types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        )
                        if reference.target_range is not None:
                            target_range = reference.target_range
                            target_selection_range = reference.target_range
                    else:
                        # CTEs and Macros always have target_range
                        target_range = reference.target_range
                        target_selection_range = reference.target_range

                    location_links.append(
                        types.LocationLink(
                            target_uri=reference.uri,
                            target_selection_range=target_selection_range,
                            target_range=target_range,
                            origin_selection_range=reference.range,
                        )
                    )
                return location_links
            except Exception as e:
                ls.show_message(f"Error getting references: {e}", types.MessageType.Error)
                return []

        @self.server.feature(types.TEXT_DOCUMENT_REFERENCES)
        def find_references(
            ls: LanguageServer, params: types.ReferenceParams
        ) -> t.Optional[t.List[types.Location]]:
            """Find all references of a symbol (supporting CTEs, models for now)"""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)

                all_references = get_all_references(context, uri, params.position)

                # Convert references to Location objects
                locations = [types.Location(uri=ref.uri, range=ref.range) for ref in all_references]

                return locations if locations else None
            except Exception as e:
                ls.show_message(f"Error getting locations: {e}", types.MessageType.Error)
                return None

        @self.server.feature(types.TEXT_DOCUMENT_DIAGNOSTIC)
        def diagnostic(
            ls: LanguageServer, params: types.DocumentDiagnosticParams
        ) -> types.DocumentDiagnosticReport:
            """Handle diagnostic pull requests from the client."""
            try:
                uri = URI(params.text_document.uri)
                diagnostics, result_id = self._get_diagnostics_for_uri(uri)

                # Check if client provided a previous result ID
                if hasattr(params, "previous_result_id") and params.previous_result_id == result_id:
                    # Return unchanged report if diagnostics haven't changed
                    return types.RelatedUnchangedDocumentDiagnosticReport(
                        kind=types.DocumentDiagnosticReportKind.Unchanged,
                        result_id=str(result_id),
                    )

                return types.RelatedFullDocumentDiagnosticReport(
                    kind=types.DocumentDiagnosticReportKind.Full,
                    items=diagnostics,
                    result_id=str(result_id),
                )
            except Exception as e:
                ls.log_trace(
                    f"Error getting diagnostics: {e}",
                )
                return types.RelatedFullDocumentDiagnosticReport(
                    kind=types.DocumentDiagnosticReportKind.Full,
                    items=[],
                )

        @self.server.feature(types.WORKSPACE_DIAGNOSTIC)
        def workspace_diagnostic(
            ls: LanguageServer, params: types.WorkspaceDiagnosticParams
        ) -> types.WorkspaceDiagnosticReport:
            """Handle workspace-wide diagnostic pull requests from the client."""
            try:
                context = self._context_get_or_load()

                items: t.List[
                    t.Union[
                        types.WorkspaceFullDocumentDiagnosticReport,
                        types.WorkspaceUnchangedDocumentDiagnosticReport,
                    ]
                ] = []

                # Get all SQL and Python model files from the context
                for path, target in context.map.items():
                    if isinstance(target, ModelTarget):
                        uri = URI.from_path(path)
                        diagnostics, result_id = self._get_diagnostics_for_uri(uri)

                        # Check if we have a previous result ID for this file
                        previous_result_id = None
                        if hasattr(params, "previous_result_ids") and params.previous_result_ids:
                            for prev in params.previous_result_ids:
                                if prev.uri == uri.value:
                                    previous_result_id = prev.value
                                    break

                        if previous_result_id and previous_result_id == result_id:
                            # File hasn't changed
                            items.append(
                                types.WorkspaceUnchangedDocumentDiagnosticReport(
                                    kind=types.DocumentDiagnosticReportKind.Unchanged,
                                    result_id=str(result_id),
                                    uri=uri.value,
                                )
                            )
                        else:
                            # File has changed or is new
                            items.append(
                                types.WorkspaceFullDocumentDiagnosticReport(
                                    kind=types.DocumentDiagnosticReportKind.Full,
                                    result_id=str(result_id),
                                    uri=uri.value,
                                    items=diagnostics,
                                )
                            )

                return types.WorkspaceDiagnosticReport(items=items)

            except Exception as e:
                ls.log_trace(
                    f"Error getting workspace diagnostics: {e}",
                )
                return types.WorkspaceDiagnosticReport(items=[])

        @self.server.feature(
            types.TEXT_DOCUMENT_COMPLETION,
            types.CompletionOptions(trigger_characters=["@"]),  # advertise "@" for macros
        )
        def completion(
            ls: LanguageServer, params: types.CompletionParams
        ) -> t.Optional[types.CompletionList]:
            """Handle completion requests from the client."""
            try:
                uri = URI(params.text_document.uri)
                context = self._context_get_or_load(uri)

                # Get the document content
                content = None
                try:
                    document = ls.workspace.get_text_document(params.text_document.uri)
                    content = document.source
                except Exception:
                    pass

                # Get completions using the existing completions module
                completion_response = LSPContext.get_completions(context, uri, content)

                completion_items = []
                # Add model completions
                for model in completion_response.model_completions:
                    completion_items.append(
                        types.CompletionItem(
                            label=model.name,
                            kind=types.CompletionItemKind.Reference,
                            detail="SQLMesh Model",
                            documentation=types.MarkupContent(
                                kind=types.MarkupKind.Markdown,
                                value=model.description or "No description available",
                            )
                            if model.description
                            else None,
                        )
                    )
                # Add macro completions
                triggered_by_at = (
                    params.context is not None
                    and getattr(params.context, "trigger_character", None) == "@"
                )

                for macro in completion_response.macros:
                    macro_name = macro.name
                    insert_text = macro_name if triggered_by_at else f"@{macro_name}"

                    completion_items.append(
                        types.CompletionItem(
                            label=f"@{macro_name}",
                            insert_text=insert_text,
                            insert_text_format=types.InsertTextFormat.PlainText,
                            filter_text=macro_name,
                            kind=types.CompletionItemKind.Function,
                            detail="SQLMesh Macro",
                            documentation=macro.description,
                        )
                    )

                for keyword in completion_response.keywords:
                    completion_items.append(
                        types.CompletionItem(
                            label=keyword,
                            kind=types.CompletionItemKind.Keyword,
                            detail="SQL Keyword",
                        )
                    )

                return types.CompletionList(
                    is_incomplete=False,
                    items=completion_items,
                )

            except Exception as e:
                get_sql_completions(None, URI(params.text_document.uri))
                return None

    def _get_diagnostics_for_uri(self, uri: URI) -> t.Tuple[t.List[types.Diagnostic], int]:
        """Get diagnostics for a specific URI, returning (diagnostics, result_id).

        Since we no longer track version numbers, we always return 0 as the result_id.
        This means pull diagnostics will always fetch fresh results.
        """
        try:
            context = self._context_get_or_load(uri)
            diagnostics = context.lint_model(uri)
            return SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(diagnostics), 0
        except Exception:
            return [], 0

    def _context_get_or_load(self, document_uri: t.Optional[URI] = None) -> LSPContext:
        if self.lsp_context is None:
            self._ensure_context_for_document(document_uri)
        if self.lsp_context is None:
            raise RuntimeError("No context found able to get or load")
        return self.lsp_context

    def _ensure_context_for_document(
        self,
        document_uri: t.Optional[URI] = None,
    ) -> None:
        """
        Ensure that a context exists for the given document if applicable by searching
        for a config.py or config.yml file in the parent directories.
        """
        if document_uri is not None:
            document_path = document_uri.to_path()
            if document_path.is_file() and document_path.suffix in (".sql", ".py"):
                document_folder = document_path.parent
                if document_folder.is_dir():
                    self._ensure_context_in_folder(document_folder)
                    return

        return self._ensure_context_in_folder()

    def _ensure_context_in_folder(self, folder_path: t.Optional[Path] = None) -> None:
        if self.lsp_context is not None:
            return

        # If not found in the provided folder, search through all workspace folders
        for workspace_folder in self.workspace_folders:
            for ext in ("py", "yml", "yaml"):
                config_path = workspace_folder / f"config.{ext}"
                if config_path.exists():
                    if self._create_lsp_context([workspace_folder]):
                        return

        #  Then , check the provided folder recursively
        path = folder_path
        if path is None:
            path = Path.cwd()
        while path.is_dir():
            for ext in ("py", "yml", "yaml"):
                config_path = path / f"config.{ext}"
                if config_path.exists():
                    if self._create_lsp_context([path]):
                        return

            path = path.parent
            if path == path.parent:
                break

        raise RuntimeError(
            f"No context found in workspaces folders {self.workspace_folders}"
            + (f" or in {folder_path}" if folder_path else "")
        )

    def _create_lsp_context(self, paths: t.List[Path]) -> t.Optional[LSPContext]:
        """Create a new LSPContext instance using the configured context class.

        On success, sets self.lsp_context and returns the created context.

        Args:
            paths: List of paths to pass to the context constructor

        Returns:
            A new LSPContext instance wrapping the created context, or None if creation fails
        """
        try:
            if self.lsp_context is None:
                context = self.context_class(paths=paths)
                loaded_sqlmesh_message(self.server, paths[0])
            else:
                self.lsp_context.context.load()
                context = self.lsp_context.context
            self.lsp_context = LSPContext(context)
            return self.lsp_context
        except Exception as e:
            # Only show the error message once
            if not self.has_raised_loading_error:
                self.server.show_message(
                    f"Error creating context: {e}",
                    types.MessageType.Error,
                )
                self.has_raised_loading_error = True

            self.server.log_trace(f"Error creating context: {e}")
            return None

    @staticmethod
    def _diagnostic_to_lsp_diagnostic(
        diagnostic: AnnotatedRuleViolation,
    ) -> t.Optional[types.Diagnostic]:
        if diagnostic.model._path is None:
            return None
        if not diagnostic.violation_range:
            with open(diagnostic.model._path, "r", encoding="utf-8") as file:
                lines = file.readlines()
            range = types.Range(
                start=types.Position(line=0, character=0),
                end=types.Position(line=len(lines) - 1, character=len(lines[-1])),
            )
        else:
            range = types.Range(
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
            range=range,
            message=diagnostic.violation_msg,
            severity=types.DiagnosticSeverity.Error
            if diagnostic.violation_type == "error"
            else types.DiagnosticSeverity.Warning,
            source="sqlmesh",
            code=diagnostic.rule.name,
            code_description=types.CodeDescription(href=rule_uri),
        )

    @staticmethod
    def _diagnostics_to_lsp_diagnostics(
        diagnostics: t.List[AnnotatedRuleViolation],
    ) -> t.List[types.Diagnostic]:
        """
        Converts a list of AnnotatedRuleViolations to a list of LSP diagnostics. It will remove duplicates based on the message and range.
        """
        lsp_diagnostics = {}
        for diagnostic in diagnostics:
            lsp_diagnostic = SQLMeshLanguageServer._diagnostic_to_lsp_diagnostic(diagnostic)
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
    def _uri_to_path(uri: str) -> Path:
        """Convert a URI to a path."""
        return URI(uri).to_path()

    def start(self) -> None:
        """Start the server with I/O transport."""
        logging.basicConfig(level=logging.DEBUG)
        self.server.start_io()


def loaded_sqlmesh_message(ls: LanguageServer, folder: Path) -> None:
    ls.show_message(
        f"Loaded SQLMesh context from {folder}",
        types.MessageType.Info,
    )


def main() -> None:
    # Example instantiator that just uses the same signature as your original `Context` usage.
    sqlmesh_server = SQLMeshLanguageServer(context_class=Context)
    sqlmesh_server.start()


if __name__ == "__main__":
    main()
