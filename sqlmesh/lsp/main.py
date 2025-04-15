#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration, refactored without globals."""

import itertools
import logging
import typing as t
from contextlib import suppress
from pathlib import Path

from lsprotocol import types
from pygls.server import LanguageServer
from pygls.workspace import TextDocument

from sqlmesh._version import __version__
from sqlmesh.core.audit.definition import ModelAudit
from sqlmesh.core.context import Context
from sqlmesh.core.model import Model


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
        self.context: t.Optional[Context] = None

        # Register LSP features (e.g., formatting, hover, etc.)
        self._register_features()

    def _register_features(self) -> None:
        """Register LSP features on the internal LanguageServer instance."""

        @self.server.feature(types.TEXT_DOCUMENT_FORMATTING)
        def formatting(
            ls: LanguageServer, params: types.DocumentFormattingParams
        ) -> t.List[types.TextEdit]:
            """Format the document using SQLMesh `format_model_expressions`."""
            try:
                document = self.ensure_context_for_document(
                    ls.workspace.get_document(params.text_document.uri)
                )

                if self.context is None:
                    raise RuntimeError(f"No context found for document: {document.path}")

                # Perform formatting using the loaded context
                self.context.format(paths=(Path(document.path),))
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

    def ensure_context_for_document(self, document: TextDocument) -> TextDocument:
        """
        Ensure that a context exists for the given document if applicable by searching
        for a config.py or config.yml file in the parent directories.
        """
        # If the context is already loaded, check if this document belongs to it.
        if self.context is not None:
            self.context.load()  # Reload or refresh context
            return document

        # No context yet: try to find config and load it
        path = Path(document.path).resolve()
        if path.suffix not in (".sql", ".py"):
            return document

        loaded = False
        # Ascend directories to look for config
        while path.parents and not loaded:
            for ext in ("py", "yml", "yaml"):
                config_path = path / f"config.{ext}"
                if config_path.exists():
                    try:
                        # Use user-provided instantiator to build the context
                        self.context = self.context_class(paths=[path])
                        self.server.show_message(f"Context loaded for: {path}")
                        loaded = True
                        # Re-check context for document now that it's loaded
                        return self.ensure_context_for_document(document)
                    except Exception as e:
                        self.server.show_message(
                            f"Error loading context: {e}", types.MessageType.Error
                        )
            path = path.parent

        return document

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
