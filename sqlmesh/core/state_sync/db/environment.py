from __future__ import annotations

import typing as t
import pandas as pd
import json
import logging
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.state_sync.db.utils import (
    fetchall,
    fetchone,
)
from sqlmesh.core.environment import Environment, EnvironmentStatements
from sqlmesh.utils.migration import index_text_type, blob_text_type
from sqlmesh.utils.date import now_timestamp, time_like_to_str
from sqlmesh.utils.errors import SQLMeshError


logger = logging.getLogger(__name__)


class EnvironmentState:
    def __init__(
        self,
        engine_adapter: EngineAdapter,
        schema: t.Optional[str] = None,
    ):
        self.engine_adapter = engine_adapter
        self.environments_table = exp.table_("_environments", db=schema)
        self.environment_statements_table = exp.table_("_environment_statements", db=schema)

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        self._environment_columns_to_types = {
            "name": exp.DataType.build(index_type),
            "snapshots": exp.DataType.build(blob_type),
            "start_at": exp.DataType.build("text"),
            "end_at": exp.DataType.build("text"),
            "plan_id": exp.DataType.build("text"),
            "previous_plan_id": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
            "finalized_ts": exp.DataType.build("bigint"),
            "promoted_snapshot_ids": exp.DataType.build(blob_type),
            "suffix_target": exp.DataType.build("text"),
            "catalog_name_override": exp.DataType.build("text"),
            "previous_finalized_snapshots": exp.DataType.build(blob_type),
            "normalize_name": exp.DataType.build("boolean"),
            "requirements": exp.DataType.build(blob_type),
        }

        self._environment_statements_columns_to_types = {
            "environment_name": exp.DataType.build(index_type),
            "plan_id": exp.DataType.build("text"),
            "environment_statements": exp.DataType.build(blob_type),
        }

    def update_environment(self, environment: Environment) -> None:
        """Updates the environment.

        Args:
            environment: The environment
        """
        self.engine_adapter.delete_from(
            self.environments_table,
            where=exp.EQ(
                this=exp.column("name"),
                expression=exp.Literal.string(environment.name),
            ),
        )

        self.engine_adapter.insert_append(
            self.environments_table,
            _environment_to_df(environment),
            columns_to_types=self._environment_columns_to_types,
        )

    def update_environment_statements(
        self,
        environment_name: str,
        plan_id: str,
        environment_statements: t.List[EnvironmentStatements],
    ) -> None:
        """Updates the environment's statements.

        Args:
            environment_name: The environment name
            plan_id: The environment's plan ID
            environment_statements: The environment statements

        """

        self.engine_adapter.delete_from(
            self.environment_statements_table,
            where=exp.EQ(
                this=exp.column("environment_name"),
                expression=exp.Literal.string(environment_name),
            ),
        )

        if environment_statements:
            self.engine_adapter.insert_append(
                self.environment_statements_table,
                _environment_statements_to_df(environment_name, plan_id, environment_statements),
                columns_to_types=self._environment_statements_columns_to_types,
            )

    def invalidate_environment(self, name: str) -> None:
        """Invalidates the environment.

        Args:
            name: The name of the environment
        """
        name = name.lower()
        if name == c.PROD:
            raise SQLMeshError("Cannot invalidate the production environment.")

        filter_expr = exp.column("name").eq(name)

        self.engine_adapter.update_table(
            self.environments_table,
            {"expiration_ts": now_timestamp()},
            where=filter_expr,
        )

    def finalize(self, environment: Environment) -> None:
        """Finalize the target environment, indicating that this environment has been
        fully promoted and is ready for use.

        Args:
            environment: The target environment to finalize.
        """
        logger.info("Finalizing environment '%s'", environment.name)

        environment_filter = exp.column("name").eq(exp.Literal.string(environment.name))

        stored_plan_id_query = (
            exp.select("plan_id")
            .from_(self.environments_table)
            .where(environment_filter, copy=False)
            .lock(copy=False)
        )
        stored_plan_id_row = fetchone(self.engine_adapter, stored_plan_id_query)

        if not stored_plan_id_row:
            raise SQLMeshError(f"Missing environment '{environment.name}' can't be finalized")

        stored_plan_id = stored_plan_id_row[0]
        if stored_plan_id != environment.plan_id:
            raise SQLMeshError(
                f"Plan '{environment.plan_id}' is no longer valid for the target environment '{environment.name}'. "
                f"Stored plan ID: '{stored_plan_id}'. Please recreate the plan and try again"
            )

        environment.finalized_ts = now_timestamp()
        self.engine_adapter.update_table(
            self.environments_table,
            {"finalized_ts": environment.finalized_ts},
            where=environment_filter,
        )

    def delete_expired_environments(self) -> t.List[Environment]:
        """Deletes expired environments.

        Returns:
            A list of deleted environments.
        """
        now_ts = now_timestamp()
        filter_expr = exp.LTE(
            this=exp.column("expiration_ts"),
            expression=exp.Literal.number(now_ts),
        )

        rows = fetchall(
            self.engine_adapter,
            self._environments_query(
                where=filter_expr,
                lock_for_update=True,
            ),
        )
        environments = [self._environment_from_row(r) for r in rows]

        self.engine_adapter.delete_from(
            self.environments_table,
            where=filter_expr,
        )

        # Delete the expired environments' corresponding environment statements
        if expired_environments := [
            exp.EQ(this=exp.column("environment_name"), expression=exp.Literal.string(env.name))
            for env in environments
        ]:
            self.engine_adapter.delete_from(
                self.environment_statements_table,
                where=exp.or_(*expired_environments),
            )

        return environments

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return [
            self._environment_from_row(row)
            for row in fetchall(self.engine_adapter, self._environments_query())
        ]

    def get_environments_summary(self) -> t.Dict[str, int]:
        """Fetches all environment names along with expiry datetime.

        Returns:
            A dict of all environment names along with expiry datetime.
        """
        return dict(
            fetchall(
                self.engine_adapter,
                self._environments_query(required_fields=["name", "expiration_ts"]),
            ),
        )

    def get_environment(
        self, environment: str, lock_for_update: bool = False
    ) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The environment
            lock_for_update: Lock the snapshot rows for future update

        Returns:
            The environment object.
        """
        row = fetchone(
            self.engine_adapter,
            self._environments_query(
                where=exp.EQ(
                    this=exp.column("name"),
                    expression=exp.Literal.string(environment),
                ),
                lock_for_update=lock_for_update,
            ),
        )

        if not row:
            return None

        env = self._environment_from_row(row)
        return env

    def get_environment_statements(self, environment: str) -> t.List[EnvironmentStatements]:
        """Fetches the environment's statements from the environment_statements table.
        Args:
            environment: The environment name

        Returns:
            A list of the environment statements.

        """
        query = (
            exp.select(
                exp.to_identifier("environment_statements"),
            )
            .from_(self.environment_statements_table)
            .where(
                exp.EQ(
                    this=exp.column("environment_name"),
                    expression=exp.Literal.string(environment),
                )
            )
        )
        result = fetchone(engine_adapter=self.engine_adapter, query=query)
        if result and (statements := json.loads(result[0])):
            return [
                EnvironmentStatements.parse_obj(environment_statements)
                for environment_statements in statements
            ]

        return []

    def _environment_from_row(self, row: t.Tuple[str, ...]) -> Environment:
        return Environment(**{field: row[i] for i, field in enumerate(Environment.all_fields())})

    def _environments_query(
        self,
        where: t.Optional[str | exp.Expression] = None,
        lock_for_update: bool = False,
        required_fields: t.Optional[t.List[str]] = None,
    ) -> exp.Select:
        query_fields = required_fields if required_fields else Environment.all_fields()
        query = (
            exp.select(*(exp.to_identifier(field) for field in query_fields))
            .from_(self.environments_table)
            .where(where)
        )
        if lock_for_update:
            return query.lock(copy=False)
        return query


def _environment_to_df(environment: Environment) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": environment.name,
                "snapshots": json.dumps(environment.snapshot_dicts()),
                "start_at": time_like_to_str(environment.start_at),
                "end_at": time_like_to_str(environment.end_at) if environment.end_at else None,
                "plan_id": environment.plan_id,
                "previous_plan_id": environment.previous_plan_id,
                "expiration_ts": environment.expiration_ts,
                "finalized_ts": environment.finalized_ts,
                "promoted_snapshot_ids": (
                    json.dumps(environment.promoted_snapshot_id_dicts())
                    if environment.promoted_snapshot_ids is not None
                    else None
                ),
                "suffix_target": environment.suffix_target.value,
                "catalog_name_override": environment.catalog_name_override,
                "previous_finalized_snapshots": (
                    json.dumps(environment.previous_finalized_snapshot_dicts())
                    if environment.previous_finalized_snapshots is not None
                    else None
                ),
                "normalize_name": environment.normalize_name,
                "requirements": json.dumps(environment.requirements),
            }
        ]
    )


def _environment_statements_to_df(
    environment_name: str, plan_id: str, environment_statements: t.List[EnvironmentStatements]
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "environment_name": environment_name,
                "plan_id": plan_id,
                "environment_statements": json.dumps([e.dict() for e in environment_statements]),
            }
        ]
    )
