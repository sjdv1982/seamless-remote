"""Async client for Seamless databases."""

import asyncio
import json
import os
import sys
from aiohttp import ClientConnectionError
from frozendict import frozendict

from seamless import Checksum
from seamless.util.pylru import lrucache

from .client import Client, _retry_operation, close_all_clients


class DatabaseClient(Client):
    """Async client for Seamless databases."""

    url: str | None = None
    _monitor_interval = 1.0

    def __init__(self, readonly: bool):
        super().__init__(readonly)
        self._max_inflight = _parse_max_inflight()
        self._semaphores: dict[asyncio.AbstractEventLoop, asyncio.Semaphore] = {}
        self._monitor_tasks: dict[asyncio.AbstractEventLoop, asyncio.Task] = {}

    async def _init(self):
        pass

    def _validate_init(self):
        if self.url is None:
            raise ValueError("Provide a URL")
        self.url = self.url.rstrip("/")

    def _get_semaphore(self) -> asyncio.Semaphore | None:
        if self._max_inflight <= 0:
            return None
        loop = asyncio.get_running_loop()
        semaphore = self._semaphores.get(loop)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._max_inflight)
            self._semaphores[loop] = semaphore
            # Disable monitoring until we need to debug
            ### self._start_monitor(loop, semaphore)
        return semaphore

    def _start_monitor(
        self, loop: asyncio.AbstractEventLoop, semaphore: asyncio.Semaphore
    ) -> None:
        if loop in self._monitor_tasks:
            return
        max_inflight = self._max_inflight

        async def _monitor():
            was_saturated = False
            while True:
                try:
                    await asyncio.sleep(self._monitor_interval)
                except asyncio.CancelledError:
                    return
                value = getattr(semaphore, "_value", None)
                if value is None:
                    continue
                if value <= 0:
                    if not was_saturated:
                        was_saturated = True
                        print(
                            "[database_client] GET semaphore saturated "
                            f"(max_inflight={max_inflight})",
                            flush=True,
                        )
                elif value >= max_inflight and was_saturated:
                    was_saturated = False
                    print(
                        "[database_client] GET semaphore drained",
                        flush=True,
                    )

        self._monitor_tasks[loop] = loop.create_task(_monitor())

    @_retry_operation
    async def get_transformation_result(self, tf_checksum: Checksum) -> Checksum | None:
        """Return result of the transformation 'tf_checksum', if known."""
        semaphore = self._get_semaphore()
        if semaphore is None:
            return await self._get_transformation_result_unthrottled(tf_checksum)

        await semaphore.acquire()
        try:
            return await self._get_transformation_result_unthrottled(tf_checksum)
        finally:
            semaphore.release()

    async def _get_transformation_result_unthrottled(
        self, tf_checksum: Checksum
    ) -> Checksum | None:
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)

        request = {"type": "transformation", "checksum": tf_checksum.hex()}

        path = self._require_url()
        async with session_async.get(path, json=request) as response:
            if int(response.status / 100) in (4, 5):
                if response.status == 404:
                    return None
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
            result0 = await response.text()
        return Checksum(result0)

    @_retry_operation
    async def get_rev_transformations(
        self, result_checksum: Checksum
    ) -> list[Checksum] | None:
        """Return transformations that produce result_checksum, if known."""
        semaphore = self._get_semaphore()
        if semaphore is None:
            return await self._get_rev_transformations_unthrottled(result_checksum)

        await semaphore.acquire()
        try:
            return await self._get_rev_transformations_unthrottled(result_checksum)
        finally:
            semaphore.release()

    async def _get_rev_transformations_unthrottled(
        self, result_checksum: Checksum
    ) -> list[Checksum] | None:
        session_async = self._get_session()
        result_checksum = Checksum(result_checksum)
        request = {"type": "rev_transformations", "checksum": result_checksum.hex()}
        path = self._require_url()
        async with session_async.get(path, json=request) as response:
            if int(response.status / 100) in (4, 5):
                if response.status == 404:
                    return None
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
            result0 = await response.text()
        try:
            payload = json.loads(result0)
        except Exception as exc:
            raise ClientConnectionError(
                f"Malformed response for rev_transformations: {result0!r}"
            ) from exc
        if payload is None:
            return None
        if not isinstance(payload, list):
            raise ClientConnectionError(
                f"Malformed response for rev_transformations: {payload!r}"
            )
        return [Checksum(item) for item in payload]

    @_retry_operation
    async def set_transformation_result(
        self, tf_checksum: Checksum, result_checksum: Checksum
    ):
        """Set <result_checksum> as the transformation result of <tf_checksum>"""
        if self.readonly:
            raise AttributeError("Read-only database client")
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)
        result_checksum = Checksum(result_checksum)

        request = {
            "type": "transformation",
            "checksum": tf_checksum.hex(),
            "value": result_checksum.hex(),
        }
        path = self._require_url()
        async with session_async.put(path, json=request) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")

    @_retry_operation
    async def undo_transformation_result(
        self, tf_checksum: Checksum, result_checksum: Checksum
    ) -> bool:
        """Contest a transformation result in the database."""
        if self.readonly:
            raise AttributeError("Read-only database client")
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)
        result_checksum = Checksum(result_checksum)

        request = {
            "type": "contest",
            "checksum": tf_checksum.hex(),
            "result": result_checksum.hex(),
        }
        path = self._require_url()
        async with session_async.put(path, json=request) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
        return True


_launcher_cache = lrucache(1000)


class DatabaseLaunchedClient(DatabaseClient):
    launch_config: dict

    def config(
        self, cluster: str, project: str, subproject: str | None, stage: str | None
    ):
        import seamless_config.tools

        mode = "ro" if self.readonly else "rw"
        self.launch_config = seamless_config.tools.configure_database(
            cluster=cluster,
            project=project,
            subproject=subproject,
            stage=stage,
            mode=mode,
        )

    def _do_init(self):
        import remote_http_launcher

        conf = self.launch_config

        frozenconf = frozendict(conf)
        server_config = _launcher_cache.get(frozenconf)
        if server_config is None:
            print("Launch database server...", file=sys.stderr)
            server_config = remote_http_launcher.run(conf)
            _launcher_cache[frozenconf] = server_config
        hostname = server_config["hostname"]
        port = server_config["port"]
        url = f"http://{hostname}:{port}"
        self.url = url

    async def _init(self):
        self._do_init()

    def ensure_initialized_sync(self, *, skip_healthcheck: bool = False):
        """Synchronously ensure initialization."""
        if self._initialized:
            return
        self._do_init()
        self._initialized = True


def _parse_max_inflight() -> int:
    default = 30
    raw = os.environ.get("SEAMLESS_DATABASE_MAX_INFLIGHT", default)
    try:
        value = int(raw)
    except Exception:
        return default
    return max(0, value)
