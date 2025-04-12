#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration."""

import logging
import typing as t
from contextlib import suppress
from pathlib import Path

from sqlmesh.core.audit.definition import ModelAudit
from sqlmesh.core.context import Context
from lsprotocol import types
from pygls.server import LanguageServer
from pygls.workspace import TextDocument
from sqlmesh._version import __version__
from sqlmesh.core.model import Model

logger = logging.getLogger(__name__)

GLOBAL_CONTEXT: t.Optional[Context,] = None
FILE_MAP: t.Dict[str, t.Union[Model, ModelAudit]] = {}


server = LanguageServer("sqlmesh_lsp", __version__)


def ensure_context_for_document(document: TextDocument) -> TextDocument:
    """Ensure that a context exists for the given document if applicable by searching for a config.py or config.yml file in the parent directories."""
    # If the context is already loaded, return the document, if it is part of the same context
    global GLOBAL_CONTEXT, FILE_MAP
    if GLOBAL_CONTEXT is not None:
        GLOBAL_CONTEXT.load()
        if document.uri in FILE_MAP:
            return document
        else:
            for model in GLOBAL_CONTEXT._models.values():
                if model._path is None:
                    continue
                path = model._path.resolve()
                if path == document.path:
                    FILE_MAP[document.uri] = model
                    return document
            for audit in GLOBAL_CONTEXT._audits.values():
                if audit._path is None:
                    continue
                path = audit._path.resolve()
                if path == document.path:
                    FILE_MAP[document.uri] = audit
                    return document
            return document

    # If there is no context, load the context and then call this function again
    path = Path(document.path).resolve()
    if path.suffix not in (".sql", ".py"):
        return document
    loaded = False
    while path.parents and not loaded:
        for ext in ("py", "yml", "yaml"):
            config_path = path / f"config.{ext}"
            if config_path.exists():
                with suppress(Exception):
                    GLOBAL_CONTEXT = Context(paths=[path])
                    server.show_message(f"Context loaded for: {path}")
                    loaded = True
                    return ensure_context_for_document(document)
        path = path.parent

    return document


@server.feature(types.TEXT_DOCUMENT_FORMATTING)
def formatting(
    ls: LanguageServer, params: types.DocumentFormattingParams
) -> t.List[types.TextEdit]:
    """Format the document using SQLMesh format_model_expressions."""
    try:
        document = ensure_context_for_document(ls.workspace.get_document(params.text_document.uri))
        context = GLOBAL_CONTEXT
        if context is None:
            raise Exception(f"No context found for document: {document.path}")
        context.format(paths=(Path(document.path),))
        with open(document.path, "r+", encoding="utf-8") as file:
            return [
                types.TextEdit(
                    range=types.Range(
                        types.Position(0, 0),
                        types.Position(len(document.lines), len(document.lines[-1])),
                    ),
                    new_text=file.read(),
                )
            ]
    except Exception as e:
        ls.show_message(f"Error formatting SQL: {e}", types.MessageType.Error)
        return []


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    server.start_io()


if __name__ == "__main__":
    main()
