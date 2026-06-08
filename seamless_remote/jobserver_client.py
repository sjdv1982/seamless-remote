"""Async client for Seamless jobservers."""

import json
import sys
from aiohttp import ClientConnectionError
from frozendict import frozendict

from seamless import Checksum
from seamless.util.pylru import lrucache
from seamless_transformer.record_runtime import get_record_mode
from seamless_transformer.remote_job import parse_remote_job_written

from .client import Client, _retry_operation


class JobserverClient(Client):
    """Async client for Seamless jobservers."""

    url: str | None = None

    def __init__(self):
        super().__init__(readonly=False)

    async def _init(self):
        pass

    def _validate_init(self):
        if self.url is None:
            raise ValueError("Provide a URL")
        self.url = self.url.rstrip("/")

    @_retry_operation
    async def run_transformation(
        self,
        transformation_dict,
        *,
        tf_checksum,
        tf_dunder,
        scratch: bool,
        strict_dunder: bool = False,
    ):
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)
        request = {
            "transformation_dict": transformation_dict,
            "tf_checksum": tf_checksum.hex(),
            "tf_dunder": tf_dunder,
            "scratch": bool(scratch),
            "strict_dunder": bool(strict_dunder),
            "record": get_record_mode(),
        }

        path = self._require_url() + "/run-transformation"
        async with session_async.get(path, json=request) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
            result0 = await response.text()
        try:
            payload = json.loads(result0)
        except Exception:
            payload = None
        if isinstance(payload, dict):
            remote_job_written = payload.get("remote_job_written")
            if isinstance(remote_job_written, str):
                return {
                    "remote_job_written": remote_job_written,
                    "record_runtime": payload.get("record_runtime"),
                }
            result_checksum = payload.get("result_checksum")
            if not isinstance(result_checksum, str):
                raise ClientConnectionError(
                    f"Malformed jobserver success payload: {payload!r}"
                )
            return {
                "result_checksum": Checksum(result_checksum),
                "probe_context": payload.get("probe_context"),
                "compilation_context": payload.get("compilation_context"),
                "job_validation": payload.get("job_validation"),
                "record_runtime": payload.get("record_runtime"),
            }
        if parse_remote_job_written(result0) is not None:
            return result0
        return Checksum(result0)

    @_retry_operation
    async def cancel_transformation(self, tf_checksum):
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)
        path = self._require_url() + f"/cancel-transformation/{tf_checksum.hex()}"
        async with session_async.post(path) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
            payload = json.loads(await response.text())
        return bool(payload.get("canceled", False))

    @_retry_operation
    async def transformation_status(self, tf_checksum):
        session_async = self._get_session()
        tf_checksum = Checksum(tf_checksum)
        path = self._require_url() + f"/transformation-status/{tf_checksum.hex()}"
        async with session_async.get(path) as response:
            if int(response.status / 100) in (4, 5):
                text = await response.text()
                raise ClientConnectionError(f"Error {response.status}: {text}")
            payload = json.loads(await response.text())
        status = payload.get("status")
        if not isinstance(status, str):
            raise ClientConnectionError(
                f"Malformed jobserver status payload: {payload!r}"
            )
        return status


_launcher_cache = lrucache(1000)


class JobserverLaunchedClient(JobserverClient):
    launch_config: dict

    def config(
        self,
        cluster: str,
        project: str,
        subproject: str | None,
        stage: str | None,
        substage: str | None,
    ):
        import seamless_config.tools

        self.launch_config = seamless_config.tools.configure_jobserver(
            cluster=cluster,
            project=project,
            subproject=subproject,
            stage=stage,
            substage=substage,
        )

    def _do_init(self):
        import remote_http_launcher

        conf = self.launch_config

        def make_frozendict(d):
            dd = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    vv = make_frozendict(v)
                elif isinstance(v, list):
                    vv = []
                    for item in v:
                        if isinstance(item, dict):
                            item2 = make_frozendict(item)
                        else:
                            item2 = item
                        vv.append(item2)
                    vv = tuple(vv)
                else:
                    vv = v
                dd[k] = vv
            return frozendict(dd)

        frozenconf = make_frozendict(conf)
        server_config = _launcher_cache.get(frozenconf)
        if server_config is None:
            print("Launch jobserver...", file=sys.stderr)
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
