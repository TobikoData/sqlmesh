#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration, refactored without globals."""

import logging
import typing as t
from pathlib import Path

from lsprotocol import types
from pygls.server import LanguageServer

from sqlmesh._version import __version__
from sqlmesh.core.context import Context
from sqlmesh.core.linter.definition import AnnotatedRuleViolation
from sqlmesh.lsp.completions import get_sql_completions
from sqlmesh.lsp.context import LSPContext
from sqlmesh.lsp.custom import ALL_MODELS_FEATURE, AllModelsRequest, AllModelsResponse
from sqlmesh.lsp.reference import get_model_definitions_for_a_path


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
        self.lint_cache: t.Dict[str, t.List[AnnotatedRuleViolation]] = {}

        # Register LSP features (e.g., formatting, hover, etc.)
        self._register_features()

    def _register_features(self) -> None:
        """Register LSP features on the internal LanguageServer instance."""

        @self.server.feature(ALL_MODELS_FEATURE)
        def all_models(ls: LanguageServer, params: AllModelsRequest) -> AllModelsResponse:
            try:
                context = self._context_get_or_load(params.textDocument.uri)
                return get_sql_completions(context, params.textDocument.uri)
            except Exception as e:
                return get_sql_completions(None, params.textDocument.uri)

        @self.server.feature(types.TEXT_DOCUMENT_DID_OPEN)
        def did_open(ls: LanguageServer, params: types.DidOpenTextDocumentParams) -> None:
            context = self._context_get_or_load(params.text_document.uri)
            if self.lint_cache.get(params.text_document.uri) is not None:
                ls.publish_diagnostics(
                    params.text_document.uri,
                    SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(
                        self.lint_cache[params.text_document.uri]
                    ),
                )
                return
            models = context.map[params.text_document.uri]
            if models is None:
                return
            self.lint_cache[params.text_document.uri] = context.context.lint_models(
                models,
                raise_on_error=False,
            )
            ls.publish_diagnostics(
                params.text_document.uri,
                SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(
                    self.lint_cache[params.text_document.uri]
                ),
            )

        @self.server.feature(types.TEXT_DOCUMENT_DID_CHANGE)
        def did_change(ls: LanguageServer, params: types.DidChangeTextDocumentParams) -> None:
            context = self._context_get_or_load(params.text_document.uri)
            models = context.map[params.text_document.uri]
            if models is None:
                return
            self.lint_cache[params.text_document.uri] = context.context.lint_models(
                models,
                raise_on_error=False,
            )
            ls.publish_diagnostics(
                params.text_document.uri,
                SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(
                    self.lint_cache[params.text_document.uri]
                ),
            )

        @self.server.feature(types.TEXT_DOCUMENT_DID_SAVE)
        def did_save(ls: LanguageServer, params: types.DidSaveTextDocumentParams) -> None:
            context = self._context_get_or_load(params.text_document.uri)
            models = context.map[params.text_document.uri]
            if models is None:
                return
            self.lint_cache[params.text_document.uri] = context.context.lint_models(
                models,
                raise_on_error=False,
            )
            ls.publish_diagnostics(
                params.text_document.uri,
                SQLMeshLanguageServer._diagnostics_to_lsp_diagnostics(
                    self.lint_cache[params.text_document.uri]
                ),
            )

        @self.server.feature(types.TEXT_DOCUMENT_FORMATTING)
        def formatting(
            ls: LanguageServer, params: types.DocumentFormattingParams
        ) -> t.List[types.TextEdit]:
            """Format the document using SQLMesh `format_model_expressions`."""
            try:
                self._ensure_context_for_document(params.text_document.uri)
                document = ls.workspace.get_document(params.text_document.uri)
                if self.lsp_context is None:
                    raise RuntimeError(f"No context found for document: {document.path}")

                # Perform formatting using the loaded context
                self.lsp_context.context.format(paths=(Path(document.path),))
                with open(document.path, "r+", encoding="utf-8") as file:
                    new_text = file.read()

                # Return a single edit that replaces the entire file.
                return [
                    types.TextEdit(
                        range=types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(
                                line=len(document.lines),
                                character=len(document.lines[-1]) if document.lines else 0,
                            ),
                        ),
                        new_text=new_text,
                    )
                ]
            except Exception as e:
                ls.show_message(f"Error formatting SQL: {e}", types.MessageType.Error)
                return []

        @self.server.feature(types.TEXT_DOCUMENT_DEFINITION)
        def goto_definition(
            ls: LanguageServer, params: types.DefinitionParams
        ) -> t.List[types.LocationLink]:
            """Jump to an object's definition."""
            try:
                self._ensure_context_for_document(params.text_document.uri)
                document = ls.workspace.get_document(params.text_document.uri)
                if self.lsp_context is None:
                    raise RuntimeError(f"No context found for document: {document.path}")

                references = get_model_definitions_for_a_path(
                    self.lsp_context, params.text_document.uri
                )
                if not references:
                    return []

                return [
                    types.LocationLink(
                        target_uri=reference.uri,
                        target_selection_range=types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        ),
                        target_range=types.Range(
                            start=types.Position(line=0, character=0),
                            end=types.Position(line=0, character=0),
                        ),
                        origin_selection_range=reference.range,
                    )
                    for reference in references
                ]

            except Exception as e:
                ls.show_message(f"Error getting references: {e}", types.MessageType.Error)
                return []

    def _context_get_or_load(self, document_uri: str) -> LSPContext:
        if self.lsp_context is None:
            self._ensure_context_for_document(document_uri)
        if self.lsp_context is None:
            raise RuntimeError("No context found")
        return self.lsp_context

    def _ensure_context_for_document(
        self,
        document_uri: str,
    ) -> None:
        """
        Ensure that a context exists for the given document if applicable by searching
        for a config.py or config.yml file in the parent directories.
        """
        if self.lsp_context is not None:
            context = self.lsp_context
            context.context.load()  # Reload or refresh context
            self.lsp_context = LSPContext(context.context)
            return

        # No context yet: try to find config and load it
        path = Path(self._uri_to_path(document_uri)).resolve()
        if path.suffix not in (".sql", ".py"):
            return

        loaded = False
        # Ascend directories to look for config
        while path.parents and not loaded:
            for ext in ("py", "yml", "yaml"):
                config_path = path / f"config.{ext}"
                if config_path.exists():
                    try:
                        # Use user-provided instantiator to build the context
                        created_context = self.context_class(paths=[path])
                        self.lsp_context = LSPContext(created_context)
                        loaded = True
                        # Re-check context for document now that it's loaded
                        return self._ensure_context_for_document(document_uri)
                    except Exception as e:
                        self.server.show_message(
                            f"Error loading context: {e}", types.MessageType.Error
                        )
            path = path.parent

        return

    @staticmethod
    def _diagnostic_to_lsp_diagnostic(
        diagnostic: AnnotatedRuleViolation,
    ) -> t.Optional[types.Diagnostic]:
        if diagnostic.model._path is None:
            return None
        with open(diagnostic.model._path, "r", encoding="utf-8") as file:
            lines = file.readlines()
        return types.Diagnostic(
            range=types.Range(
                start=types.Position(line=0, character=0),
                end=types.Position(line=len(lines), character=len(lines[-1])),
            ),
            message=diagnostic.violation_msg,
            severity=types.DiagnosticSeverity.Error
            if diagnostic.violation_type == "error"
            else types.DiagnosticSeverity.Warning,
        )

    @staticmethod
    def _diagnostics_to_lsp_diagnostics(
        diagnostics: t.List[AnnotatedRuleViolation],
    ) -> t.List[types.Diagnostic]:
        lsp_diagnostics: t.List[types.Diagnostic] = []
        for diagnostic in diagnostics:
            lsp_diagnostic = SQLMeshLanguageServer._diagnostic_to_lsp_diagnostic(diagnostic)
            if lsp_diagnostic is not None:
                lsp_diagnostics.append(lsp_diagnostic)
        return lsp_diagnostics

    @staticmethod
    def _uri_to_path(uri: str) -> str:
        """Convert a URI to a path."""
        if uri.startswith("file://"):
            return Path(uri[7:]).resolve().as_posix()
        return Path(uri).resolve().as_posix()

    def start(self) -> None:
        """Start the server with I/O transport."""
        logging.basicConfig(level=logging.DEBUG)
        self.server.start_io()


def main() -> None:
    # Example instantiator that just uses the same signature as your original `Context` usage.
    sqlmesh_server = SQLMeshLanguageServer(context_class=Context)
    sqlmesh_server.start()


if __name__ == "__main__":
    main()
