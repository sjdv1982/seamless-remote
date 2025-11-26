"""Async client for Seamless databases."""

from aiohttp import ClientConnectionError
from frozendict import frozendict

from seamless import Checksum
from seamless.util.pylru import lrucache

from .client import Client, _retry_operation, close_all_clients


class DatabaseClient(Client):
    """Async client for Seamless databases."""

    url: str | None = None

    async def _init(self):
        pass

    def _validate_init(self):
        if self.url is None:
            raise ValueError("Provide a URL")
        self.url = self.url.rstrip("/")

    @_retry_operation
    async def get_transformation_result(self, tf_checksum: Checksum) -> Checksum | None:
        """Return result of the transformation 'tf_checksum', if known."""
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
            print("Launch database server...")
            server_config = remote_http_launcher.run(conf)
            _launcher_cache[frozenconf] = server_config
        hostname = server_config["hostname"]
        port = server_config["port"]
        url = f"http://{hostname}:{port}"
        self.url = url

    async def _init(self):
        self._do_init()

    def ensure_initialized_sync(self):
        """Synchronously ensure initialization."""
        if self._initialized:
            return
        self._do_init()
        self._initialized = True
