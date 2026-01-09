"""Async client for buffer read/write servers or buffer directories."""

import asyncio
import json
import os
import pathlib
import sys
import time
from typing import Optional

import aiofiles
import aiofiles.os
from aiohttp import ClientConnectionError, ClientPayloadError
from frozendict import frozendict

from seamless import Buffer, Checksum
from seamless.util.pylru import lrucache

from .client import (
    Client,
    _retry_operation,
    close_all_clients as _close_all_base_clients,
)


class BufferClient(Client):
    """Async client that can read/write buffers via HTTP or local directories."""

    url: Optional[str] = None
    directory: Optional[str] = None

    def __init__(self, readonly: bool):
        super().__init__(readonly)

    async def _init(self):
        pass

    def _validate_init(self):
        self.url = self.url.rstrip("/") if self.url is not None else None
        if self.url is None and self.directory is None:
            raise ValueError("Provide at least a url or directory")

    def _require_url(self) -> str:
        if self.url is None:
            raise ValueError("No url configured for this BufferClient")
        return self.url

    def _require_directory(self) -> str:
        if self.directory is None:
            raise ValueError("No directory configured for this BufferClient")
        return self.directory

    @_retry_operation
    async def buffer_length(self, checksum: Checksum) -> bool:
        """Return True if the buffer exists on the remote server."""
        return await self._buffer_length_unthrottled(checksum)

    async def _buffer_length_unthrottled(self, checksum: Checksum) -> bool:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        cs = checksum.hex()

        path = self._require_url() + "/has"
        async with session_async.get(path, json=[cs]) as response:
            if int(response.status / 100) in (4, 5):
                raise ClientConnectionError()
            result0 = await response.read()
        try:
            result = json.loads(result0)
            if not isinstance(result, list) or len(result) != 1:
                raise ValueError(result)
            if not isinstance(result[0], int):
                raise ValueError(result)
        except ValueError:
            print(
                "WARNING: '{}' has the wrong format for buffer length, checksum {}".format(
                    self.url, checksum
                ),
                file=sys.stderr,
            )
        return result[0]

    @_retry_operation
    async def get(self, checksum: Checksum) -> Buffer | None:
        """Download a buffer from the configured server."""
        return await self._get_unthrottled(checksum)

    async def _get_unthrottled(self, checksum: Checksum) -> Buffer | None:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        assert checksum
        curr_buf_checksum = None
        while 1:
            path = self._require_url() + "/" + str(checksum)
            async with session_async.get(path) as response:
                if int(response.status) == 404:
                    return None
                if int(response.status / 100) in (4, 5):
                    raise ClientConnectionError()
                buf0 = await response.read()
                buf = Buffer(buf0)
            buf_checksum = buf.get_checksum().hex()
            if buf_checksum != checksum:
                if buf_checksum != curr_buf_checksum:
                    curr_buf_checksum = buf_checksum
                    continue
                print(
                    "WARNING: '{}' has the wrong checksum for {}".format(
                        self.url, checksum
                    ),
                    file=sys.stderr,
                )
                return None
            return buf

    @_retry_operation
    async def promise(self, checksum: Checksum):
        """Promise that a buffer will be uploaded to the configured server."""
        if self.readonly:
            raise AttributeError("Read-only buffer client")
        await self._promise_unthrottled(checksum)

    async def _promise_unthrottled(self, checksum: Checksum) -> None:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        path = self._require_url() + "/promise/" + str(checksum)
        async with session_async.put(path) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")

    @_retry_operation
    async def write(self, checksum: Checksum, buffer: Buffer):
        """Upload a buffer to the configured server."""
        if self.readonly:
            raise AttributeError("Read-only buffer client")
        await self._write_unthrottled(checksum, buffer)

    async def _write_unthrottled(self, checksum: Checksum, buffer: Buffer) -> None:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        buffer_bytes = Buffer(buffer).content
        assert checksum
        path = self._require_url() + "/" + str(checksum)
        async with session_async.put(path, data=buffer_bytes) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")

    @_retry_operation
    async def get_file_buffer(
        self, checksum: Checksum, timeout: float = 10
    ) -> Buffer | None:
        """Read a buffer from the configured directory, verifying checksum."""
        directory = self._require_directory()
        checksum = Checksum(checksum)
        assert checksum
        filename1 = os.path.join(directory, checksum.hex())
        filenames = [filename1]
        subdirectory = os.path.join(directory, checksum.hex()[:2])
        if os.path.exists(subdirectory):
            filename2 = os.path.join(subdirectory, checksum.hex())
            filenames = [filename1, filename2]
        for filename in filenames:
            if await aiofiles.os.path.exists(filename):
                async with aiofiles.open(filename, "rb") as f:
                    buf_bytes = await f.read()
                buf = Buffer(buf_bytes)
                buf_checksum = await buf.get_checksum_async()
                if buf_checksum == checksum:
                    return buf

            global_lockfile = os.path.join(directory, ".LOCK")
            lockfile = filename + ".LOCK"
            start_time = time.time()
            while 1:
                for lockf in [global_lockfile, lockfile]:
                    if await aiofiles.os.path.exists(lockf):
                        break
                else:
                    break
                await asyncio.sleep(0.5)
                if time.time() - start_time > timeout:
                    return None
            if not await aiofiles.os.path.exists(filename):
                continue
            async with aiofiles.open(filename, "rb") as f:
                buf0 = await f.read()
            buf = Buffer(buf0)
            buf_checksum = await buf.get_checksum_async()
            if buf_checksum != checksum:
                return None
            return buf
        return None


def _close_all_clients():
    try:
        from seamless.caching import buffer_writer
    except Exception:
        buffer_writer = None
    else:
        try:
            buffer_writer.flush(timeout=30.0)
        except Exception:
            pass
    _close_all_base_clients()


_launcher_cache = lrucache(1000)


class BufferLaunchedClient(BufferClient):
    local: bool
    launch_config: dict

    def config(
        self, cluster: str, project: str, subproject: str | None, stage: str | None
    ):
        import seamless_config.tools

        mode = "ro" if self.readonly else "rw"
        self.launch_config = seamless_config.tools.configure_hashserver(
            cluster=cluster,
            project=project,
            subproject=subproject,
            stage=stage,
            mode=mode,
        )
        self.local = "hostname" not in self.launch_config

    def _do_init(self):
        import remote_http_launcher

        conf = self.launch_config
        directory = None
        if self.local:
            directory = pathlib.Path(conf["workdir"]).expanduser().as_posix()

        frozenconf = frozendict(conf)
        server_config = _launcher_cache.get(frozenconf)
        if server_config is None:
            print("Launch hashserver...")
            server_config = remote_http_launcher.run(conf)
            _launcher_cache[frozenconf] = server_config
        hostname = server_config["hostname"]
        port = server_config["port"]
        url = f"http://{hostname}:{port}"
        self.url = url
        self.directory = directory

    async def _init(self):
        self._do_init()

    def ensure_initialized_sync(self, *, skip_healthcheck: bool = False):
        """Synchronously ensure initialization."""
        if self._initialized:
            return
        self._do_init()
        self._initialized = True

