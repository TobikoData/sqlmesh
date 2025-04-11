#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration."""

import logging
import typing as t
from contextlib import suppress
from pathlib import Path

from sqlmesh.core.context import Context
from lsprotocol import types
from pygls.server import LanguageServer
from pygls.workspace import TextDocument
from sqlmesh._version import __version__
from sqlmesh.core.model import Model

logger = logging.getLogger(__name__)

CONTEXTS: t.Dict[str, Context] = {}
"""A mapping of workspace paths to SQLMesh contexts."""

PATHS_TO_MODELS: t.Dict[str, t.Tuple[Context, Model]] = {}
"""A mapping of file paths to SQLMesh (context, model) tuples."""

server = LanguageServer("sqlmesh_lsp", __version__)

_CACHE: t.Set[str] = set()
"""A cache of URIs for which we have already ensured a context exists."""


def ensure_context_for_document(document: TextDocument) -> TextDocument:
    """Ensure that a context exists for the given document if applicable by searching for a config.py or config.yml file in the parent directories."""
    if document.uri in _CACHE:
        return document
    _CACHE.add(document.uri)
    path = Path(document.path).resolve()
    if path.suffix not in (".sql", ".py"):
        return document
    initial_path = path
    while path.parents:
        if str(path) in CONTEXTS:
            return document
        path = path.parent
    path = initial_path
    loaded = False
    while path.parents and not loaded:
        for ext in ("py", "yml", "yaml"):
            config_path = path / f"config.{ext}"
            if config_path.exists():
                with suppress(Exception):
                    handle = Context(paths=[f"{path}"])
                    CONTEXTS[str(path)] = handle
                    PATHS_TO_MODELS.update(
                        {
                            str(model._path.resolve()): (handle, model)
                            for model in handle.models.values()
                        }
                    )
                    server.show_message(f"Context loaded for: {path}")
                    loaded = True
                    break
        path = path.parent
    return document


@server.feature(types.TEXT_DOCUMENT_FORMATTING)
def formatting(
    ls: LanguageServer, params: types.DocumentFormattingParams
) -> t.List[types.TextEdit]:
    """Format the document based using SQLMesh format_model_expressions."""
    try:
        document = ensure_context_for_document(ls.workspace.get_document(params.text_document.uri))
        context, _ = PATHS_TO_MODELS.get(document.path, (None, None))
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
