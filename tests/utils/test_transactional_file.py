import time
from tempfile import NamedTemporaryFile
from threading import Thread

import fsspec  # type: ignore
import pytest

from sqlmesh.utils.transactional_file import TransactionalFile


@pytest.fixture(scope="session")
def fs() -> fsspec.AbstractFileSystem:
    return fsspec.filesystem("file")


def test_file_transaction_with_content(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        # Seed the file with some content
        f.write(b"test 1")
        f.flush()
        file = TransactionalFile(f.name, fs)

        # Test that we cannot write without a lock
        with pytest.raises(RuntimeError):
            file.write(b"test 2")

        # Test that we can write with a lock
        file.acquire_lock()
        file.write(b"test 2")
        assert file.read() == b"test 2"
        assert file._original_contents == b"test 1"

        # Test that we can rollback
        file.rollback()
        assert file.read() == b"test 1"
        assert file._original_contents == b"test 1"
        file.release_lock()


def test_file_transaction_no_content(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        file = TransactionalFile(f.name, fs)
        with pytest.raises(RuntimeError):
            file.write(b"test 1")
        file.acquire_lock()
        file.write(b"test 1")
        assert file.read() == b"test 1"

        # Ensure that we can rollback to an empty file
        # if the file was empty to begin with
        assert file._original_contents == b""
        file.rollback()
        assert file.read() == b""
        file.release_lock()


def test_file_transaction_multiple_writers(fs: fsspec.AbstractFileSystem):
    with NamedTemporaryFile() as f:
        writer_1 = TransactionalFile(f.name, fs)
        writer_2 = TransactionalFile(f.name, fs)
        writer_3 = TransactionalFile(f.name, fs)
        writer_1.acquire_lock()

        # Test that we cannot acquire a lock if it is already held
        assert not writer_2.acquire_lock(blocking=False)

        # Test 1 writer and multiple readers
        writer_1.write(b"test 1")
        assert writer_1.read() == b"test 1"
        assert writer_2.read() == b"test 1"
        assert writer_3.read() == b"test 1"

        # Test handover of lock
        writer_1.release_lock()
        assert writer_2.acquire_lock()

        # Test 1 writer and multiple readers
        writer_2.write(b"test 2")
        assert writer_1.read() == b"test 2"
        assert writer_2.read() == b"test 2"
        assert writer_3.read() == b"test 2"
        writer_2.release_lock()


def test_file_transaction_concurrent_writers():
    fs = fsspec.filesystem("file")
    with NamedTemporaryFile() as f:
        writer_1 = TransactionalFile(f.name, fs)
        writer_2 = TransactionalFile(f.name, fs)
        writer_3 = TransactionalFile(f.name, fs)

        # Test lock acquisition timeout
        writer_1.acquire_lock()
        lock_acquire_timeout = 2.0
        job1 = Thread(target=writer_2.acquire_lock, args=(True, lock_acquire_timeout), daemon=True)
        job1.start()
        t1 = time.time()
        job1.join()
        t2 = time.time()
        assert t2 - t1 <= lock_acquire_timeout + 1.5
        assert not writer_2._is_locked

        # Other writers cannot acquire lock
        assert not writer_3.acquire_lock(blocking=False)
        # Multiple readers OK
        writer_1.write(b"test 1")
        assert writer_1.read() == b"test 1"
        assert writer_2.read() == b"test 1"
        assert writer_3.read() == b"test 1"
        # Verify original contents
        assert writer_1._original_contents == b""

        # Handover lock to Writer 2
        writer_1.release_lock()
        assert writer_2.acquire_lock(timeout=lock_acquire_timeout)

        # Verify new original contents
        assert writer_2._original_contents == b"test 1"

        # Test Writer 3 waits indefinitely and acquires lock after Writer 2 releases
        job2 = Thread(target=writer_3.acquire_lock, daemon=True)
        job2.start()

        # Writer 1 cannot acquire lock or write
        time.sleep(0.1)
        assert not writer_1.acquire_lock(blocking=False)
        with pytest.raises(RuntimeError):
            writer_1.write(b"this won't work")

        # Write new contents
        writer_2.write(b"test 2")
        assert writer_1.read() == b"test 2"

        # Release lock and ensure writer 3 acquires it by joining + writing
        writer_2.release_lock()
        job2.join()
        time.sleep(0.1)
        writer_3.write(b"test 3")
