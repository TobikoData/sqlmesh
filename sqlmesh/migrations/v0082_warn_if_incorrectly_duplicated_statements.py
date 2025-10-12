"""
This script's goal is to warn users if there are two adjacent expressions in a SQL
model that are equivalent.

Context:

We used to include `Semicolon` expressions in the model's state, which led to a bug
where the expression preceding the semicolon would be duplicated in pre_statements
or post_statements. For example, the query in the model below would be incorrectly
included in its post_statements list:

```
MODEL (
  name test
);

SELECT 1 AS c;

-- foo
```

We now don't include `Semicolon` expressions in the model's state, which fixes this
issue, but unfortunately migrating existing snapshots is not possible because we do
not have a signal in state to detect whether an expression was incorrectly duplicated.

If a SQL model suffered from this issue, then there would be two adjacent equivalent
expressions in it, so we use that as a heuristic to warn the user accordingly.
"""

import json

from sqlglot import exp

from sqlmesh.core.console import get_console


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    pass


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    warning = (
        "SQLMesh detected that it may not be able to fully migrate the state database. This should not impact "
        "the migration process, but may result in unexpected changes being reported by the next `sqlmesh plan` "
        "command. Please run `sqlmesh diff prod` after the migration has completed, before making any new "
        "changes. If any unexpected changes are reported, consider running a forward-only plan to apply these "
        "changes and avoid unnecessary backfills: sqlmesh plan prod --forward-only. "
        "See https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans for more details.\n"
    )

    for (snapshot,) in engine_adapter.fetchall(
        exp.select("snapshot").from_(snapshots_table), quote_identifiers=True
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        if node.get("source_type") == "sql":
            expressions = [
                *node.get("pre_statements", []),
                node["query"],
                *node.get("post_statements", []),
            ]
            for e1, e2 in zip(expressions, expressions[1:]):
                if e1 == e2:
                    get_console().log_warning(warning)
                    return
