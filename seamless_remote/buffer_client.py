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

    @_retry_operation
    async def buffer_lengths(self, checksums: list[Checksum]) -> list[int | None]:
        """Return buffer lengths for multiple checksums."""
        return await self._buffer_lengths_unthrottled(checksums)

    async def _buffer_length_unthrottled(self, checksum: Checksum) -> bool:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        cs = checksum.hex()

        path = self._require_url() + "/has"
        async with session_async.get(path, json=[cs]) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(
                    f"buffer_length failed for checksum {cs}: "
                    f"HTTP {response.status} from {path}: {text}"
                )
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

    async def _buffer_lengths_unthrottled(
        self, checksums: list[Checksum]
    ) -> list[int | None]:
        if not checksums:
            return []

        if self.directory is not None and self.url is None:
            directory = self._require_directory()
            results: list[int | None] = []
            for checksum in checksums:
                checksum = Checksum(checksum)
                if not checksum:
                    results.append(None)
                    continue
                cs_hex = checksum.hex()
                path = os.path.join(directory, cs_hex)
                try:
                    stat = await aiofiles.os.stat(path)
                except FileNotFoundError:
                    stat = None
                if stat is None:
                    subdir = os.path.join(directory, cs_hex[:2])
                    path = os.path.join(subdir, cs_hex)
                    try:
                        stat = await aiofiles.os.stat(path)
                    except FileNotFoundError:
                        stat = None
                results.append(stat.st_size if stat is not None else None)
            return results

        session_async = self._get_session()
        checksums = [Checksum(checksum) for checksum in checksums]
        cs_list = [checksum.hex() for checksum in checksums]

        path = self._require_url() + "/has"
        async with session_async.get(path, json=cs_list) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(
                    f"buffer_lengths failed for {len(cs_list)} checksums: "
                    f"HTTP {response.status} from {path}: {text}"
                )
            result0 = await response.read()
        result = [None] * len(cs_list)
        try:
            result = json.loads(result0)
            if not isinstance(result, list) or len(result) != len(cs_list):
                raise ValueError(result)
            if not all(isinstance(item, int) for item in result):
                raise ValueError(result)
        except ValueError:
            print(
                "WARNING: '{}' has the wrong format for buffer lengths".format(
                    self.url
                ),
                file=sys.stderr,
            )
        return result

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
            path = self._require_url() + "/" + checksum.hex()
            async with session_async.get(path) as response:
                if int(response.status) == 404:
                    return None
                if int(response.status / 100) in (4, 5):
                    text = await response.text()
                    raise ClientConnectionError(
                        f"GET buffer failed for checksum {checksum}: "
                        f"HTTP {response.status} from {path}: {text}"
                    )
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
        path = self._require_url() + "/promise/" + checksum.hex()
        async with session_async.put(path, timeout=self._request_timeout()) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")

    @_retry_operation
    async def write(self, checksum: Checksum, buffer: Buffer):
        """Upload a buffer to the configured server."""
        if self.readonly:
            raise AttributeError("Read-only buffer client")
        return await self._write_unthrottled(checksum, buffer)

    async def _write_unthrottled(self, checksum: Checksum, buffer: Buffer) -> bool:
        session_async = self._get_session()
        checksum = Checksum(checksum)
        buffer_bytes = Buffer(buffer).content
        assert checksum
        path = self._require_url() + "/" + checksum.hex()
        async with session_async.put(
            path, data=buffer_bytes, timeout=self._request_timeout()
        ) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
        return True

    @_retry_operation
    async def get_file_buffer(
        self, checksum: Checksum, timeout: float = 10
    ) -> Buffer | None:
        """Read and validate a buffer from the configured directory."""
        return await asyncio.to_thread(
            self.get_file_buffer_sync, checksum, timeout
        )

    def _file_buffer_candidates(self, checksum: Checksum):
        directory = self._require_directory()
        checksum = Checksum(checksum)
        return (
            os.path.join(directory, checksum.hex()),
            os.path.join(directory, checksum.hex()[:2], checksum.hex()),
        )

    def has_file_buffer(self, checksum: Checksum) -> bool:
        """Check configured folder metadata without reading buffer contents."""
        return any(
            os.path.isfile(filename)
            for filename in self._file_buffer_candidates(checksum)
        )

    def get_file_buffer_length(self, checksum: Checksum) -> int | None:
        """Return a configured folder file's size without reading its contents."""
        for filename in self._file_buffer_candidates(checksum):
            try:
                stat = os.stat(filename)
            except FileNotFoundError:
                continue
            if os.path.isfile(filename):
                return stat.st_size
        return None

    def get_file_buffer_sync(
        self, checksum: Checksum, timeout: float = 10
    ) -> Buffer | None:
        """Synchronously read and verify a buffer from a configured folder."""
        directory = self._require_directory()
        checksum = Checksum(checksum)
        assert checksum
        global_lockfile = os.path.join(directory, ".LOCK")
        for filename in self._file_buffer_candidates(checksum):
            if os.path.exists(filename):
                try:
                    with open(filename, "rb") as f:
                        content = f.read()
                except FileNotFoundError:
                    content = None
                if content is not None:
                    buffer = Buffer(content)
                    if buffer.get_checksum() == checksum:
                        return buffer

            start_time = time.time()
            lockfile = filename + ".LOCK"
            while os.path.exists(global_lockfile) or os.path.exists(lockfile):
                if time.time() - start_time > timeout:
                    return None
                time.sleep(0.5)
            if not os.path.exists(filename):
                continue
            try:
                with open(filename, "rb") as f:
                    content = f.read()
            except FileNotFoundError:
                continue
            buffer = Buffer(content)
            if buffer.get_checksum() != checksum:
                return None
            return buffer
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
        if self.local:
            self.directory = pathlib.Path(
                self.launch_config["workdir"]
            ).expanduser().as_posix()
        else:
            self.directory = None

    def _do_init(self):
        import remote_http_launcher

        conf = self.launch_config
        directory = None
        if self.local:
            directory = pathlib.Path(conf["workdir"]).expanduser().as_posix()

        frozenconf = frozendict(conf)
        server_config = _launcher_cache.get(frozenconf)
        if server_config is None:
            print("Launch hashserver...", file=sys.stderr)
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
