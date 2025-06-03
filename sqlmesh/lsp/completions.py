from functools import lru_cache
from sqlglot import Dialect, Tokenizer
from sqlmesh.lsp.custom import AllModelsResponse
import typing as t
from sqlmesh.lsp.context import AuditTarget, LSPContext, ModelTarget
from sqlmesh.lsp.uri import URI


def get_sql_completions(
    context: t.Optional[LSPContext], file_uri: t.Optional[URI]
) -> AllModelsResponse:
    """
    Return a list of completions for a given file.
    """
    return AllModelsResponse(
        models=list(get_models(context, file_uri)),
        keywords=list(get_keywords(context, file_uri)),
    )


def get_models(context: t.Optional[LSPContext], file_uri: t.Optional[URI]) -> t.Set[str]:
    """
    Return a list of models for a given file.

    If there is no context, return an empty list.
    If there is a context, return a list of all models bar the ones the file itself defines.
    """
    if context is None:
        return set()

    all_models = set()
    # Extract model names from ModelInfo objects
    for file_info in context.map.values():
        if isinstance(file_info, ModelTarget):
            all_models.update(file_info.names)

    # Remove models from the current file
    path = file_uri.to_path() if file_uri is not None else None
    if path is not None and path in context.map:
        file_info = context.map[path]
        if isinstance(file_info, ModelTarget):
            for model in file_info.names:
                all_models.discard(model)

    return all_models


def get_keywords(context: t.Optional[LSPContext], file_uri: t.Optional[URI]) -> t.Set[str]:
    """
    Return a list of sql keywords for a given file.
    If no context is provided, return ANSI SQL keywords.

    If a context is provided but no file_uri is provided, returns the keywords
    for the default dialect of the context.

    If both a context and a file_uri are provided, returns the keywords
    for the dialect of the model that the file belongs to.
    """
    if file_uri is not None and context is not None and file_uri.to_path() in context.map:
        file_info = context.map[file_uri.to_path()]

        # Handle ModelInfo objects
        if isinstance(file_info, ModelTarget) and file_info.names:
            model_name = file_info.names[0]
            model_from_context = context.context.get_model(model_name)
            if model_from_context is not None and model_from_context.dialect:
                return get_keywords_from_tokenizer(model_from_context.dialect)

        # Handle AuditInfo objects
        elif isinstance(file_info, AuditTarget) and file_info.name:
            audit = context.context.standalone_audits.get(file_info.name)
            if audit is not None and audit.dialect:
                return get_keywords_from_tokenizer(audit.dialect)

    if context is not None:
        return get_keywords_from_tokenizer(context.context.default_dialect)

    return get_keywords_from_tokenizer(None)


@lru_cache()
def get_keywords_from_tokenizer(dialect: t.Optional[str] = None) -> t.Set[str]:
    """
    Return a list of sql keywords for a given dialect. This is separate from
    the direct use of Tokenizer.KEYWORDS.keys() because that returns a set of
    keywords that are expanded, e.g. "ORDER BY" -> ["ORDER", "BY"].
    """
    tokenizer = Tokenizer
    if dialect is not None:
        try:
            tokenizer = Dialect.get_or_raise(dialect).tokenizer_class
        except Exception:
            pass

    expanded_keywords = set()
    for keyword in tokenizer.KEYWORDS.keys():
        parts = keyword.split(" ")
        expanded_keywords.update(parts)
    return expanded_keywords
