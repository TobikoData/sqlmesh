import argparse
import logging
import os
import tempfile

from pyspark import SparkFiles
from pyspark.sql import SparkSession

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.snapshot import SnapshotEvaluator
from sqlmesh.engines import commands
from sqlmesh.engines.spark.db_api import spark_session as spark_session_db
from sqlmesh.engines.spark.db_api.errors import NotSupportedError
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


def get_or_create_spark_session(dialect: str) -> SparkSession:
    if dialect == "databricks":
        spark = SparkSession.getActiveSession()
        if not spark:
            raise SQLMeshError("Could not find an active SparkSession.")
        return spark
    return (
        SparkSession.builder.config("spark.scheduler.mode", "FAIR")
        .enableHiveSupport()
        .getOrCreate()
    )


def main(
    dialect: str,
    default_catalog: str,
    command_type: commands.CommandType,
    ddl_concurrent_tasks: int,
    payload_path: str,
) -> None:
    if dialect not in ("databricks", "spark"):
        raise NotSupportedError(
            f"Dialect '{dialect}' not supported. Must be either 'databricks' or 'spark'"
        )
    logging.basicConfig(
        format="%(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
        level=logging.INFO,
    )
    command_handler = commands.COMMAND_HANDLERS.get(command_type)
    if not command_handler:
        raise NotSupportedError(f"Command '{command_type.value}' not supported")

    spark = get_or_create_spark_session(dialect)

    evaluator = SnapshotEvaluator(
        create_engine_adapter(
            lambda: spark_session_db.connection(spark),
            dialect,
            default_catalog=default_catalog,
            multithreaded=ddl_concurrent_tasks > 1,
            execute_log_level=logging.INFO,
        ),
        ddl_concurrent_tasks=ddl_concurrent_tasks,
    )
    if dialect == "spark":
        with open(SparkFiles.get(payload_path), "r", encoding="utf-8") as payload_fd:
            command_payload = payload_fd.read()
    else:
        from pyspark.dbutils import DBUtils  # type: ignore

        dbutils = DBUtils(spark)
        with tempfile.TemporaryDirectory() as tmp:
            local_payload_path = os.path.join(tmp, commands.COMMAND_PAYLOAD_FILE_NAME)
            dbutils.fs.cp(payload_path, f"file://{local_payload_path}")
            with open(local_payload_path, "r", encoding="utf-8") as payload_fd:
                command_payload = payload_fd.read()
    logger.info("Command payload:\n %s", command_payload)
    command_handler(evaluator, command_payload)

    evaluator.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQLMesh Spark Submit App")
    parser.add_argument(
        "--dialect",
        help="The dialect to use when creating the engine adapter.",
    )
    parser.add_argument(
        "--default_catalog",
        help="The default catalog to use when creating the engine adapter.",
    )
    parser.add_argument(
        "--command_type",
        type=commands.CommandType,
        choices=list(commands.CommandType),
        help="The type of command that is being run",
    )
    parser.add_argument(
        "--ddl_concurrent_tasks",
        type=int,
        default=1,
        help="The number of ddl concurrent tasks to use. Default to 1.",
    )
    parser.add_argument(
        "--payload_path",
        help="Path to the payload object. Can be a local or remote path.",
    )
    args = parser.parse_args()
    main(
        args.dialect,
        args.default_catalog,
        args.command_type,
        args.ddl_concurrent_tasks,
        args.payload_path,
    )
