import asyncio
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request

from seamless import Buffer, Checksum
from seamless.caching import buffer_writer
from seamless.checksum.calculate_checksum import calculate_checksum
from seamless.checksum.hash_type import (
    from_buffer,
    get_hash_type,
    get_hash_type_cache,
)
from seamless.checksum.hash_type_validation import (
    ensure_hash_type,
    ensure_hash_type_async,
)
from seamless.checksum.parse_buffer import parse_buffer_sync
from seamless_remote import database_remote
from seamless_remote.database_client import DatabaseClient


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_server(port, process):
    url = f"http://127.0.0.1:{port}/healthcheck"
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("database server exited before becoming ready")
        try:
            with urllib.request.urlopen(url, timeout=0.2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("database server did not become ready")


def test_sync_hash_type_validation_uses_real_remote_database(tmp_path, monkeypatch):
    payload = b"42"
    buffer = Buffer(payload)
    checksum = Checksum(calculate_checksum(payload))
    expected = from_buffer(payload)

    cache = get_hash_type_cache()
    cache.pop(checksum, None)

    port = _free_port()
    database_dir = Path(__file__).parents[2] / "seamless-database"
    database_file = tmp_path / "database.sqlite"
    environment = os.environ.copy()
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from database import main; main()",
            str(database_file),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--writable",
        ],
        cwd=database_dir,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = DatabaseClient(readonly=False)
    client.url = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(database_remote, "_read_database_clients", [client])
    monkeypatch.setattr(database_remote, "_write_database_clients", [client])

    try:
        _wait_for_server(port, process)
        asyncio.run(client.set_hash_type(checksum, expected.word))

        assert get_hash_type(checksum) is None
        assert ensure_hash_type(checksum) == expected
        assert get_hash_type(checksum) == expected

        cache.pop(checksum, None)

        async def parse_inside_running_loop():
            # With no local entry and no supplied buffer, sync lookup must not
            # try to run an async remote request inside this event loop.
            assert ensure_hash_type(checksum) is None

            # The async API does perform the real remote lookup in the loop.
            assert await ensure_hash_type_async(checksum) == expected
            assert get_hash_type(checksum) == expected
            cache.pop(checksum, None)

            # Supplying bytes lets the sync parse path classify locally without
            # blocking the loop on the database.
            return parse_buffer_sync(buffer, checksum, "int", copy=True)

        assert asyncio.run(parse_inside_running_loop()) == 42
        assert get_hash_type(checksum) == expected
        buffer_writer.flush(timeout=5)
    finally:
        try:
            buffer_writer.flush(timeout=5)
        finally:
            cache.pop(checksum, None)
            client._close_sessions()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
