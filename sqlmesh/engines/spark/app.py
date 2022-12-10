import logging
import sys

from pyspark import SparkFiles
from pyspark.sql import SparkSession

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.engines import commands
from sqlmesh.engines.spark.db_api import spark_session as spark_session_db
from sqlmesh.engines.spark.db_api.errors import NotSupportedError

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.config("spark.scheduler.mode", "FAIR")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    logging.basicConfig(
        format="%(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
        level=logging.INFO,
    )

    command_type = commands.CommandType(sys.argv[1])
    command_handler = commands.COMMAND_HANDLERS.get(command_type)
    if not command_handler:
        raise NotSupportedError(f"Command '{command_type.value}' not supported")

    spark = create_spark_session()

    ddl_concurrent_tasks = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    evaluator = SnapshotEvaluator(
        EngineAdapter(
            lambda: spark_session_db.connection(spark),
            "spark",
            multithreaded=ddl_concurrent_tasks > 1,
        ),
        ddl_concurrent_tasks=ddl_concurrent_tasks,
    )

    with open(SparkFiles.get(commands.COMMAND_PAYLOAD_FILE_NAME), "r") as payload_fd:
        command_payload = payload_fd.read()
        logger.info("Command payload:\n %s", command_payload)

    command_handler(evaluator, command_payload)

    evaluator.close()


if __name__ == "__main__":
    main()
