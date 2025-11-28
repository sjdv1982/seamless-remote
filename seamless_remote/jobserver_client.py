"""Async client for Seamless jobservers."""

from frozendict import frozendict

from seamless.util.pylru import lrucache

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
    ):
        raise NotImplementedError


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
            print("Launch jobserver...")
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
