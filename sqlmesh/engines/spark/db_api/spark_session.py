import logging
import typing as t
from threading import get_ident

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from sqlmesh.engines.spark.db_api.errors import NotSupportedError, ProgrammingError

logger = logging.getLogger(__name__)


class SparkSessionCursor:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._last_df: t.Optional[DataFrame] = None
        self._last_output: t.Optional[t.List[t.Tuple]] = None
        self._last_output_cursor: int = 0

    def execute(self, query: str, parameters: t.Optional[t.Any] = None) -> None:
        if parameters:
            raise NotSupportedError("Parameterized queries are not supported")

        self._last_df = self._spark.sql(query)
        self._last_output = None
        self._last_output_cursor = 0

    def fetchone(self) -> t.Optional[t.Tuple]:
        result = self._fetch(size=1)
        return result[0] if result else None

    def fetchmany(self, size: int = 1) -> t.List[t.Tuple]:
        return self._fetch(size=size)

    def fetchall(self) -> t.List[t.Tuple]:
        return self._fetch()

    def close(self) -> None:
        pass

    def fetchdf(self) -> t.Optional[DataFrame]:
        return self._last_df

    def _fetch(self, size: t.Optional[int] = None) -> t.List[t.Tuple]:
        if size and size < 0:
            raise ProgrammingError("The size argument can't be negative")

        if self._last_df is None:
            raise ProgrammingError("No call to .execute() has been issued")

        if self._last_output is None:
            self._last_output = _normalize_rows(self._last_df.collect())

        if self._last_output_cursor >= len(self._last_output):
            return []

        if size is None:
            size = len(self._last_output) - self._last_output_cursor

        output = self._last_output[self._last_output_cursor : self._last_output_cursor + size]
        self._last_output_cursor += size

        return output


class SparkSessionConnection:
    def __init__(self, spark: SparkSession, catalog: t.Optional[str] = None):
        self.spark = spark
        self.catalog = catalog

    @property
    def _spark_major_minor(self) -> t.Tuple[int, int]:
        return tuple(int(x) for x in self.spark.version.split(".")[:2])  # type: ignore

    def get_current_catalog(self) -> t.Optional[str]:
        if self._spark_major_minor >= (3, 4):
            return self.spark.catalog.currentCatalog()
        return self.catalog or "spark_catalog"

    def set_current_catalog(self, catalog_name: str) -> None:
        if self._spark_major_minor >= (3, 4):
            return self.spark.catalog.setCurrentCatalog(catalog_name)
        current_catalog = self.get_current_catalog()
        if current_catalog != catalog_name:
            logger.warning(
                "Spark <3.4 does not support certain cross catalog queries since the default catalog cannot be set <3.4"
            )

    def cursor(self) -> SparkSessionCursor:
        try:
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"pool_{get_ident()}")
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            self.spark.conf.set("hive.exec.dynamic.partition", "true")
            self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        except NotImplementedError:
            # Databricks Connect does not support accessing the SparkContext nor does it support
            # setting dynamic partition overwrite since it uses replace where
            pass
        if self.catalog:
            from py4j.protocol import Py4JError

            try:
                self.set_current_catalog(self.catalog)
            # Databricks does not support `setCurrentCatalog` with Unity catalog
            # and shared clusters so we use the Databricks Unity only SQL command instead
            except Py4JError:
                self.spark.sql(f"USE CATALOG {self.catalog}")
        return SparkSessionCursor(self.spark)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def connection(spark: SparkSession, catalog: t.Optional[str] = None) -> SparkSessionConnection:
    return SparkSessionConnection(spark, catalog)


def _normalize_rows(rows: t.Sequence[Row]) -> t.List[t.Tuple]:
    return [tuple(r) for r in rows]
