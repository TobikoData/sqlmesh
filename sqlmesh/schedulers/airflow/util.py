from __future__ import annotations

import contextlib
import json
import logging
import typing as t
from datetime import timedelta

from airflow import settings
from airflow.api.common.experimental.delete_dag import delete_dag
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import BaseOperator, DagRun, DagTag, XCom
from airflow.models.connection import Connection
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from sqlmesh.core import constants as c
from sqlmesh.core.config import parse_connection_config
from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.state_sync import CachingStateSync, EngineAdapterStateSync, StateSync
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


# Used to omit Optional for session instances supplied by
# Airflow at runtime. This makes the type signature cleaner
# and prevents mypy from complaining.
PROVIDED_SESSION: Session = t.cast(Session, None)


SQLMESH_STATE_CONN_ID = "sqlmesh_state_db"


@contextlib.contextmanager
def scoped_state_sync() -> t.Iterator[StateSync]:
    state_schema = c.SQLMESH
    try:
        connection = Connection.get_connection_from_secrets(SQLMESH_STATE_CONN_ID)

        connection_config_dict = json.loads(connection.extra)
        state_schema = connection_config_dict.pop("state_schema", state_schema)
        if "type" not in connection_config_dict:
            logger.info(
                "SQLMesh connection in Airflow did not have type defined. "
                "Therefore using Airflow database connection"
            )
            raise AirflowException

        logger.info("Using connection '%s' for state sync", connection.conn_id)

        connection_config = parse_connection_config(connection_config_dict)
        engine_adapter = connection_config.create_engine_adapter()
    except AirflowException:
        logger.info("Using the Airflow database connection for state sync")

        dialect = settings.engine.dialect.name
        engine_adapter = create_engine_adapter(
            settings.engine.raw_connection, dialect, multithreaded=True
        )

    try:
        yield CachingStateSync(EngineAdapterStateSync(engine_adapter, state_schema))  # type: ignore
    finally:
        engine_adapter.close()


@provide_session
def get_snapshot_dag_ids(session: Session = PROVIDED_SESSION) -> t.List[str]:
    dag_tags = session.query(DagTag).filter(DagTag.name == common.SNAPSHOT_AIRFLOW_TAG).all()
    return [tag.dag_id for tag in dag_tags]


@provide_session
def get_finished_plan_application_dag_ids(
    ttl: t.Optional[timedelta] = None, session: Session = PROVIDED_SESSION
) -> t.Set[str]:
    dag_ids = (
        session.query(DagTag.dag_id)
        .join(DagRun, DagTag.dag_id == DagRun.dag_id)
        .filter(
            DagTag.name == common.PLAN_AIRFLOW_TAG,
            DagRun.state.in_((DagRunState.SUCCESS, DagRunState.FAILED)),
        )
    )
    if ttl is not None:
        dag_ids = dag_ids.filter(DagRun.last_scheduling_decision <= now() - ttl)
    return {dag_id[0] for dag_id in dag_ids.all()}


@provide_session
def delete_dags(dag_ids: t.Set[str], session: Session = PROVIDED_SESSION) -> None:
    for dag_id in dag_ids:
        try:
            delete_dag(dag_id, session=session)
        except DagNotFound:
            logger.warning("DAG '%s' was not found", dag_id)
        except AirflowException:
            logger.warning("Failed to delete DAG '%s'", dag_id, exc_info=True)


@provide_session
def delete_xcoms(
    dag_id: str,
    keys: t.Set[str],
    task_id: t.Optional[str] = None,
    run_id: t.Optional[str] = None,
    session: Session = PROVIDED_SESSION,
) -> None:
    query = session.query(XCom).filter(XCom.dag_id == dag_id, XCom.key.in_(keys))
    if task_id is not None:
        query = query.filter_by(task_id=task_id)
    if run_id is not None:
        query = query.filter_by(run_id=run_id)
    query.delete(synchronize_session=False)


def discover_engine_operator(name: str, sql_only: bool = False) -> t.Type[BaseOperator]:
    name = name.lower()

    try:
        if name == "clickhouse":
            from sqlmesh.schedulers.airflow.operators.clickhouse import SQLMeshClickHouseOperator

            return SQLMeshClickHouseOperator
        if name == "spark":
            from sqlmesh.schedulers.airflow.operators.spark_submit import (
                SQLMeshSparkSubmitOperator,
            )

            return SQLMeshSparkSubmitOperator
        if name in ("databricks", "databricks-submit", "databricks-sql"):
            if name == "databricks-submit" or (name == "databricks" and not sql_only):
                from sqlmesh.schedulers.airflow.operators.databricks import (
                    SQLMeshDatabricksSubmitOperator,
                )

                return SQLMeshDatabricksSubmitOperator
            if name == "databricks-sql" or (name == "databricks" and sql_only):
                from sqlmesh.schedulers.airflow.operators.databricks import (
                    SQLMeshDatabricksSQLOperator,
                )

                return SQLMeshDatabricksSQLOperator
        if name == "snowflake":
            from sqlmesh.schedulers.airflow.operators.snowflake import (
                SQLMeshSnowflakeOperator,
            )

            return SQLMeshSnowflakeOperator
        if name == "bigquery":
            from sqlmesh.schedulers.airflow.operators.bigquery import (
                SQLMeshBigQueryOperator,
            )

            return SQLMeshBigQueryOperator
        if name == "redshift":
            from sqlmesh.schedulers.airflow.operators.redshift import (
                SQLMeshRedshiftOperator,
            )

            return SQLMeshRedshiftOperator
        if name in ("postgres", "postgresql"):
            from sqlmesh.schedulers.airflow.operators.postgres import (
                SQLMeshPostgresOperator,
            )

            return SQLMeshPostgresOperator
        if name == "trino":
            from sqlmesh.schedulers.airflow.operators.trino import SQLMeshTrinoOperator

            return SQLMeshTrinoOperator
        if name == "mssql":
            from sqlmesh.schedulers.airflow.operators.mssql import SQLMeshMsSqlOperator

            return SQLMeshMsSqlOperator

        if name == "mysql":
            from sqlmesh.schedulers.airflow.operators.mysql import SQLMeshMySqlOperator

            return SQLMeshMySqlOperator
    except ImportError:
        raise SQLMeshError(f"Failed to automatically discover an operator for '{name}'.'")

    raise ValueError(f"Unsupported engine name '{name}'.")
