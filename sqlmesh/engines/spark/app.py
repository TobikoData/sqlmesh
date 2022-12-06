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
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def main() -> None:
    logging.basicConfig(
        format="%(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
        level=logging.INFO,
    )

    spark = create_spark_session()
    connection = spark_session_db.connection(spark)
    evaluator = SnapshotEvaluator(EngineAdapter(connection, "spark"))

    command_type = commands.CommandType(sys.argv[1])
    command_handler = commands.COMMAND_HANDLERS.get(command_type)
    if not command_handler:
        raise NotSupportedError(f"Command '{command_type.value}' not supported")

    with open(SparkFiles.get(commands.COMMAND_PAYLOAD_FILE_NAME), "r") as payload_fd:
        command_payload = payload_fd.read()
        logger.info("Command payload:\n %s", command_payload)

    command_handler(evaluator, command_payload)


if __name__ == "__main__":
    main()
