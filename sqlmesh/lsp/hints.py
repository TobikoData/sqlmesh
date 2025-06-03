"""Type hinting on SQLMesh models"""

import typing as t

from lsprotocol import types

from sqlglot import exp
from sqlglot.expressions import Expression
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.uri import URI


def get_hints(
    lsp_context: LSPContext,
    document_uri: URI,
    start_line: int,
    end_line: int,
) -> t.List[types.InlayHint]:
    """
    Get type hints for certain lines in a document

    Args:
        lint_context: The LSP context
        document_uri: The URI of the document
        start_line: the starting line to get hints for
        end_line: the ending line to get hints for

    Returns:
        A list of hints to apply to the document
    """
    path = document_uri.to_path()
    if path.suffix != ".sql":
        return []

    if path not in lsp_context.map:
        return []

    file_info = lsp_context.map[path]

    # Process based on whether it's a model or standalone audit
    if isinstance(file_info, ModelTarget):
        # It's a model
        model = lsp_context.context.get_model(
            model_or_snapshot=file_info.names[0], raise_if_missing=False
        )
        if model is None or not isinstance(model, SqlModel):
            return []

        query = model.query
        dialect = model.dialect
        columns_to_types = model.columns_to_types or {}
    else:
        return []

    return _get_type_hints_for_model_from_query(
        query, dialect, columns_to_types, start_line, end_line
    )


def _get_type_hints_for_model_from_query(
    query: Expression,
    dialect: str,
    columns_to_types: t.Dict[str, exp.DataType],
    start_line: int,
    end_line: int,
) -> t.List[types.InlayHint]:
    hints: t.List[types.InlayHint] = []
    try:
        query = normalize_identifiers(query.copy(), dialect=dialect)
        root = build_scope(query)

        if not root:
            return []

        for select in find_all_in_scope(root.expression, exp.Select):
            for select_exp in select.expressions:
                if not select_exp:
                    continue

                if isinstance(select_exp, exp.Alias):
                    meta = select_exp.args["alias"]._meta
                elif isinstance(select_exp, exp.Column):
                    meta = select_exp.parts[-1]._meta
                else:
                    continue

                line = meta.get("line") - 1  # Lines from sqlglot are 1 based
                col = meta.get("col")

                name = select_exp.alias_or_name
                if name not in columns_to_types:
                    continue

                if line < start_line or line > end_line:
                    continue

                data_type = columns_to_types.get(name)

                if not data_type or data_type.is_type(exp.DataType.Type.UNKNOWN):
                    continue

                type_label = str(data_type)
                hints.append(
                    types.InlayHint(
                        label=f"::{type_label}",
                        kind=types.InlayHintKind.Type,
                        padding_left=False,
                        padding_right=True,
                        position=types.Position(line=line, character=col),
                    )
                )

        return hints
    except Exception:
        return []
