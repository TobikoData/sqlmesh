import typing as t
import pytest
from threading import current_thread, Thread
import random
from sqlglot import exp
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.utils.connection_pool import ThreadLocalSharedConnectionPool

pytestmark = [pytest.mark.duckdb, pytest.mark.engine, pytest.mark.slow]


@pytest.mark.parametrize("database", [None, "db.db"])
def test_multithread_concurrency(tmp_path: Path, database: t.Optional[str]):
    num_threads = 100

    if database:
        database = str(tmp_path / database)

    config = DuckDBConnectionConfig(concurrent_tasks=8, database=database)

    adapter = config.create_engine_adapter()

    assert isinstance(adapter._connection_pool, ThreadLocalSharedConnectionPool)

    # this test loosely follows this example: https://duckdb.org/docs/guides/python/multiple_threads.html
    adapter.execute(
        "create table tbl (thread_name varchar, insert_time timestamp default current_timestamp)"
    )

    # list.append() is threadsafe
    write_results = []
    read_results = []

    def write_from_thread():
        thread_name = str(current_thread().name)
        query = exp.insert(
            exp.values([(exp.Literal.string(thread_name),)]), "tbl", columns=["thread_name"]
        )
        adapter.execute(query)
        adapter.execute(f"CREATE TABLE thread_{thread_name} (id int)")
        write_results.append(thread_name)

    def read_from_thread():
        thread_name = str(current_thread().name)
        query = exp.select(
            exp.Literal.string(thread_name).as_("thread_name"),
            exp.Count(this="*").as_("row_counter"),
            exp.CurrentTimestamp(),
        ).from_("tbl")
        results = adapter.fetchall(query)
        assert len(results) == 1
        read_results.append(results[0])

    threads = []

    for i in range(num_threads):
        threads.append(Thread(target=write_from_thread, name=f"write_thread_{i}"))
        threads.append(Thread(target=read_from_thread, name=f"read_thread_{i}"))

    random.seed(6)
    random.shuffle(threads)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(read_results) == num_threads
    assert len(write_results) == num_threads

    tables = adapter.fetchall("show tables")
    assert len(tables) == num_threads + 1


def test_secret_registration_from_multiple_connections(tmp_path: Path):
    database = str(tmp_path / "db.db")

    config = DuckDBConnectionConfig(
        database=database,
        concurrent_tasks=2,
        secrets={"s3": {"type": "s3", "region": "us-east-1", "key_id": "foo", "secret": "bar"}},
    )

    adapter = config.create_engine_adapter()
    pool = adapter._connection_pool

    assert isinstance(pool, ThreadLocalSharedConnectionPool)

    def _open_connection() -> bool:
        # this triggers cursor_init() to be run again for the new connection from the new thread
        # if the operations in cursor_init() are not idempotent, DuckDB will throw an error and this test will fail
        cur = pool.get_cursor()
        cur.execute("SELECT name FROM duckdb_secrets()")
        secret_names = [name for name_row in cur.fetchall() for name in name_row]
        assert secret_names == ["s3"]
        return True

    thread_pool = ThreadPoolExecutor(max_workers=4)
    futures = []
    for _ in range(10):
        futures.append(thread_pool.submit(_open_connection))

    for future in as_completed(futures):
        assert future.result()


def test_connector_config_from_multiple_connections(tmp_path: Path):
    config = DuckDBConnectionConfig(
        concurrent_tasks=2,
        extensions=["tpch"],
        connector_config={"temp_directory": str(tmp_path), "memory_limit": "16mb"},
    )

    adapter = config.create_engine_adapter()
    pool = adapter._connection_pool

    assert isinstance(pool, ThreadLocalSharedConnectionPool)

    adapter.execute("CALL dbgen(sf = 0.1)")

    # check that temporary files exist so that calling "SET temp_directory = 'anything'" will throw an error
    assert len(adapter.fetchall("select path from duckdb_temporary_files()")) > 0

    def _open_connection() -> bool:
        # This triggers cursor_init() which should only SET values if they have changed
        pool.get_cursor()
        return True

    thread_pool = ThreadPoolExecutor(max_workers=4)
    futures = []
    for _ in range(4):
        futures.append(thread_pool.submit(_open_connection))

    for future in as_completed(futures):
        assert future.result()

    pool.close_all()
