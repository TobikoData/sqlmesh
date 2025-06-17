from __future__ import annotations
import jinja2.nodes as j
from sqlglot import exp
import typing as t
import sqlmesh.core.constants as c
from pathlib import Path


# jinja transform is a function that takes (current node, previous node, parent node) and returns a new Node or None
# returning None means the current node is removed from the tree
# returning a different Node means the current node is replaced with the new Node
JinjaTransform = t.Callable[[j.Node, t.Optional[j.Node], t.Optional[j.Node]], t.Optional[j.Node]]
SQLGlotTransform = t.Callable[[exp.Expression], t.Optional[exp.Expression]]


def _sqlmesh_predefined_macro_variables() -> t.Set[str]:
    def _gen() -> t.Iterable[str]:
        for suffix in ("dt", "date", "ds", "ts", "tstz", "hour", "epoch", "millis"):
            for prefix in ("start", "end", "execution"):
                yield f"{prefix}_{suffix}"

        for item in ("runtime_stage", "gateway", "this_model", "this_env", "model_kind_name"):
            yield item

    return set(_gen())


SQLMESH_PREDEFINED_MACRO_VARIABLES = _sqlmesh_predefined_macro_variables()


def infer_dbt_package_from_path(path: Path) -> t.Optional[str]:
    """
    Given a path like "sqlmesh-project/macros/__dbt_packages__/foo/bar.sql"

    Infer that 'foo' is the DBT package
    """
    if c.MIGRATED_DBT_PACKAGES in path.parts:
        idx = path.parts.index(c.MIGRATED_DBT_PACKAGES)
        return path.parts[idx + 1]
    return None
