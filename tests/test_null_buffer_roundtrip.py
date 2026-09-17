import asyncio
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request

from seamless import Buffer, Checksum
from seamless.checksum.null import NULL_CHECKSUM
from seamless_remote.buffer_client import BufferClient


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_server(port, process):
    url = f"http://127.0.0.1:{port}/healthcheck"
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("hashserver exited before becoming ready")
        try:
            with urllib.request.urlopen(url, timeout=0.2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("hashserver did not become ready")


def test_null_buffer_uploads_and_downloads_from_hashserver(tmp_path):
    port = _free_port()
    hashserver_dir = Path(__file__).parents[2] / "hashserver"
    environment = os.environ.copy()
    environment.update(
        {
            "HASHSERVER_DIRECTORY": str(tmp_path),
            "HASHSERVER_LAYOUT": "flat",
            "HASHSERVER_WRITABLE": "1",
        }
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "hashserver:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=hashserver_dir,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_server(port, process)

        async def roundtrip():
            client = BufferClient(readonly=False)
            client.url = f"http://127.0.0.1:{port}"
            client._validate_init()
            buffer = Buffer(None, "plain")
            checksum = buffer.get_checksum()
            assert checksum == Checksum(NULL_CHECKSUM)
            await client.write(checksum, buffer)
            downloaded = await client.get(checksum)
            assert downloaded is not None
            assert downloaded.content == b"null\n"
            client._close_sessions()

        asyncio.run(roundtrip())
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
