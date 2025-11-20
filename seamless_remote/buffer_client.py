"""Async client for buffer read/write servers or buffer directories."""

import asyncio
import atexit
import json
import os
import pathlib
import sys
import threading
import time
from typing import Optional
import weakref
from functools import wraps

import aiofiles
import aiofiles.os
import aiohttp
from aiohttp import ClientConnectionError, ClientPayloadError
from frozendict import frozendict

from seamless import Buffer, Checksum
from seamless.util.pylru import lrucache

_clients = weakref.WeakSet()


RETRYABLE_EXCEPTIONS = (
    ClientConnectionError,
    ClientPayloadError,
    asyncio.TimeoutError,
    FileNotFoundError,
)


def _retry_operation(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        for attempt in range(5):
            try:
                await self._wait_for_init()
                return await method(self, *args, **kwargs)
            except RETRYABLE_EXCEPTIONS:
                self._initialized = False
                if attempt == 4:
                    raise
        raise RuntimeError("Unreachable")

    return wrapper


class BufferClient:
    """Async client that can read/write buffers via HTTP or local directories."""

    readonly: bool
    url: Optional[str] = None
    directory: Optional[str] = None

    def __init__(self, readonly: bool):
        self.readonly = readonly
        self._initialized = False
        self._sessions = weakref.WeakKeyDictionary()
        _clients.add(self)

    async def _init(self):
        pass

    def _validate_init(self):
        self.url = self.url.rstrip("/") if self.url is not None else None
        if self.url is None and self.directory is None:
            raise ValueError("Provide at least a url or directory")

    async def _wait_for_init(self):
        if self._initialized:
            return
        await self._init()
        self._validate_init()
        await self.healthcheck()
        self._initialized = True

    def _require_url(self) -> str:
        if self.url is None:
            raise ValueError("No url configured for this BufferClient")
        return self.url

    def _require_directory(self) -> str:
        if self.directory is None:
            raise ValueError("No directory configured for this BufferClient")
        return self.directory

    def _get_session(self) -> aiohttp.ClientSession:
        self._require_url()
        thread = threading.current_thread()
        session_async = self._sessions.get(thread)
        if session_async is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop != session_async._loop:
                    session_async = None
            except RuntimeError:
                session_async = None
        if session_async is None:
            timeout = aiohttp.ClientTimeout(total=10)
            session_async = aiohttp.ClientSession(timeout=timeout)
            self._sessions[thread] = session_async
        return session_async

    @_retry_operation
    async def buffer_length(self, checksum: Checksum) -> bool:
        """Return True if the buffer exists on the remote server."""
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
    async def get(self, checksum: Checksum) -> bytes | None:
        """Download a buffer from the configured server."""
        session_async = self._get_session()
        checksum = Checksum(checksum)
        assert checksum
        curr_buf_checksum = None
        while 1:
            path = self._require_url() + "/" + str(checksum)
            async with session_async.get(path) as response:
                if int(response.status / 100) in (4, 5):
                    raise ClientConnectionError()
                buf = await response.read()
            buf_checksum = Buffer(buf).get_checksum().hex()
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
    async def write(self, checksum: Checksum, buffer: Buffer | bytes):
        """Upload a buffer to the configured server."""
        if self.readonly:
            raise AttributeError("Read-only buffer client")
        session_async = self._get_session()
        checksum = Checksum(checksum)
        buffer_bytes = Buffer(buffer).content
        assert checksum
        path = self._require_url() + "/" + str(checksum)
        async with session_async.put(path, data=buffer_bytes) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")

    async def healthcheck(self) -> None:
        """Verify that the remote server responds successfully to /healthcheck."""
        if self.url is None:
            return
        session_async = self._get_session()
        path = self._require_url() + "/healthcheck"
        try:
            async with session_async.get(path) as response:
                if not (200 <= response.status < 300):
                    text = await response.text()
                    raise RuntimeError(
                        f"Healthcheck failed for {self.url}: HTTP {response.status} {text}"
                    )
        except ClientConnectionError as exc:
            raise RuntimeError(f"Healthcheck failed for {self.url}: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Healthcheck failed for {self.url}: {exc}") from exc

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

    def _close_sessions(self):
        sessions = list(self._sessions.values())
        self._sessions.clear()
        for session in sessions:
            _close_session(session)


def _close_all_clients():
    for client in list(_clients):
        client._close_sessions()


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

    async def _init(self):
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


atexit.register(_close_all_clients)


def _close_session(session: aiohttp.ClientSession) -> None:
    """Close a session on the event loop it belongs to."""
    loop = getattr(session, "_loop", None)
    if loop is None or loop.is_closed():
        return
    coro = session.close()

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if loop is running_loop:
        if loop.is_running():
            loop.create_task(coro)
        else:
            loop.run_until_complete(coro)
        return

    if loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            fut.result()
        except Exception:
            pass
        return

    thread_id = getattr(loop, "_thread_id", None)
    if thread_id == threading.get_ident():
        loop.run_until_complete(coro)
        return
    # Loop is stopped and owned by another thread; nothing to do safely
