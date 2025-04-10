#!/usr/bin/env python
"""A Language Server Protocol (LSP) server for SQL with SQLMesh integration."""

import asyncio
import gc
import logging
import typing as t
import weakref
from collections import defaultdict
from contextlib import suppress
from itertools import cycle
from pathlib import Path

import sqlmesh
from lsprotocol import types
from pygls.server import LanguageServer
from pygls.workspace import TextDocument
from sqlmesh._version import __version__

logger = logging.getLogger(__name__)

CONTEXTS: t.Dict[str, sqlmesh.Context] = {}
"""A mapping of workspace paths to SQLMesh contexts."""

PATHS_TO_MODELS: t.Dict[str, t.Tuple[sqlmesh.Context, sqlmesh.Model]] = {}
"""A mapping of file paths to SQLMesh (context, model) tuples."""

C_MUTEX: t.DefaultDict[t.Union[str, Path], asyncio.Lock] = defaultdict(asyncio.Lock)
"""A locking mechanism for ensuring that context mutation is thread-safe."""

loop = asyncio.get_event_loop()

server = LanguageServer("sqlmesh_lsp", __version__, loop=loop)


async def refresh_context_loop(context: sqlmesh.Context) -> None:
    """Refresh the SQLMesh context every 5 seconds.
    SQLMesh already ensures that the context is only refreshed when necessary so this
    is efficient and safe to do, even if the context is large. Mtime checks are used.
    """
    gc_iter = cycle(list(range(10)))
    while True:
        await asyncio.sleep(10.0)
        for loader in context._loaders:
            if loader.reload_needed():
                async with C_MUTEX[context.path]:
                    await asyncio.to_thread(context.load)
                PATHS_TO_MODELS.update(
                    {
                        str(model._path.resolve()): (context, weakref.proxy(model))
                        for model in context.models.values()
                    }
                )
        if next(gc_iter) == 0:
            gc.collect()


_CACHE = set()
"""A cache of URIs for which we have already ensured a context exists."""


async def ensure_context_for_document(document: TextDocument) -> TextDocument:
    """Ensure that a context exists for the given document if applicable."""
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
                    handle = sqlmesh.Context(paths=path)
                    loop.create_task(refresh_context_loop(handle))
                    CONTEXTS[str(path)] = handle
                    PATHS_TO_MODELS.update(
                        {
                            str(model._path.resolve()): (handle, weakref.proxy(model))
                            for model in handle.models.values()
                        }
                    )
                    server.show_message(f"Context loaded for: {path}")
                    loaded = True
                    break
        path = path.parent
    return document


@server.feature(types.TEXT_DOCUMENT_FORMATTING)
async def formatting(
    ls: LanguageServer, params: types.DocumentFormattingParams
) -> t.List[types.TextEdit]:
    """Format the document based using SQLMesh format_model_expressions."""
    try:
        logger.info(f"Formatting document: {params.text_document.uri}")
        document = await ensure_context_for_document(
            ls.workspace.get_document(params.text_document.uri)
        )
        context, model = PATHS_TO_MODELS.get(document.path, (None, None))
        context.format(paths=[Path(document.path)])
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


@server.feature(types.TEXT_DOCUMENT_DID_OPEN)
async def did_open(ls: LanguageServer, params: types.DidOpenTextDocumentParams) -> None:
    """Update diagnostics on document open and refresh context if necessary."""
    document = await ensure_context_for_document(
        ls.workspace.get_document(params.text_document.uri)
    )
    path = Path(document.path)
    known_paths = PATHS_TO_MODELS.keys()
    for context in CONTEXTS.values():
        if (
            path.is_relative_to(context.path)
            and path.suffix in (".sql", ".py")
            and str(path) not in known_paths
        ):
            ls.show_message(f"Refreshing context with new file: {path}", types.MessageType.Info)
            async with C_MUTEX[context.path]:
                await asyncio.to_thread(context.load)
                PATHS_TO_MODELS.update(
                    {
                        str(model._path.resolve()): (context, weakref.proxy(model))
                        for model in context.models.values()
                    }
                )


@server.feature(types.TEXT_DOCUMENT_DID_SAVE)
async def did_save(ls: LanguageServer, params: types.DidOpenTextDocumentParams) -> None:
    """Update diagnostics on document save."""
    document = await ensure_context_for_document(
        ls.workspace.get_document(params.text_document.uri)
    )
    context, _ = PATHS_TO_MODELS.get(document.path, (None, None))
    if context is not None:
        for loader in context._loaders:
            loader._path_mtimes[Path(document.path)] = 0.0
            async with C_MUTEX[context.path]:
                await asyncio.to_thread(context.load)
            for model in context.models.values():
                if model._path == Path(document.path):
                    PATHS_TO_MODELS[document.path] = (context, weakref.proxy(model))
                    break


@server.feature(types.WORKSPACE_DID_CHANGE_WATCHED_FILES)
async def did_change_watched_files(
    ls: LanguageServer, params: types.DidChangeWatchedFilesParams
) -> None:
    """Refresh context if a file changes."""
    updated: t.Dict[t.Union[str, Path], bool] = {}
    for change in params.changes:
        document = await ensure_context_for_document(ls.workspace.get_text_document(change.uri))
        path = Path(document.path)
        known_paths = PATHS_TO_MODELS.keys()
        if change.type == types.FileChangeType.Deleted and str(path) in known_paths:
            # We don't need to refresh the context if a file is deleted, we just remove it from the cache
            del PATHS_TO_MODELS[str(path)]
            continue
        for context in CONTEXTS.values():
            # If a new file is created, we need to force reload the appropriate context
            if (
                path.is_relative_to(context.path)
                and path.suffix in (".sql", ".py")
                and str(path) not in known_paths
                and change.type == types.FileChangeType.Created
                and not updated.get(context.path, False)
            ):
                ls.show_message(f"Refreshing context with new file: {path}", types.MessageType.Info)
                async with C_MUTEX[context.path]:
                    await asyncio.to_thread(context.load)
                    PATHS_TO_MODELS.update(
                        {
                            str(model._path.resolve()): (context, weakref.proxy(model))
                            for model in context.models.values()
                        }
                    )
                    updated[context.path] = True


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    server.start_io()


if __name__ == "__main__":
    main()
