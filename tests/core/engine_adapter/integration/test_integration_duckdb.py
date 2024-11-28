import typing as t
import pytest
from threading import current_thread, Thread
import random
from sqlglot import exp

from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.utils.connection_pool import ThreadLocalConnectionPool

pytestmark = [pytest.mark.duckdb, pytest.mark.engine, pytest.mark.slow]


@pytest.mark.parametrize("database", [None, "db.db"])
def test_multithread_concurrency(tmp_path, database: t.Optional[str]):
    num_threads = 100

    if database:
        database = str(tmp_path / database)

    config = DuckDBConnectionConfig(concurrent_tasks=8, database=database)

    adapter = config.create_engine_adapter()

    assert isinstance(adapter._connection_pool, ThreadLocalConnectionPool)

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
