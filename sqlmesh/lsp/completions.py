from functools import lru_cache
from sqlglot import Dialect, Tokenizer
from sqlmesh.lsp.custom import AllModelsResponse
import typing as t
from sqlmesh.lsp.context import LSPContext


def get_sql_completions(context: t.Optional[LSPContext], file_uri: str) -> AllModelsResponse:
    """
    Return a list of completions for a given file.
    """
    return AllModelsResponse(
        models=list(get_models(context, file_uri)),
        keywords=list(get_keywords(context, file_uri)),
    )


def get_models(context: t.Optional[LSPContext], file_uri: t.Optional[str]) -> t.Set[str]:
    """
    Return a list of models for a given file.

    If there is no context, return an empty list.
    If there is a context, return a list of all models bar the ones the file itself defines.
    """
    if context is None:
        return set()
    all_models = set(model for models in context.map.values() for model in models)
    if file_uri is not None:
        models_file_refers_to = context.map[file_uri]
        for model in models_file_refers_to:
            all_models.discard(model)
    return all_models


def get_keywords(context: t.Optional[LSPContext], file_uri: t.Optional[str]) -> t.Set[str]:
    """
    Return a list of sql keywords for a given file.
    If no context is provided, return ANSI SQL keywords.

    If a context is provided but no file_uri is provided, returns the keywords
    for the default dialect of the context.

    If both a context and a file_uri are provided, returns the keywords
    for the dialect of the model that the file belongs to.
    """
    if file_uri is not None and context is not None:
        models = context.map[file_uri]
        if models:
            model = models[0]
            model_from_context = context.context.get_model(model)
            if model_from_context is not None:
                if model_from_context.dialect:
                    return get_keywords_from_tokenizer(model_from_context.dialect)
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
