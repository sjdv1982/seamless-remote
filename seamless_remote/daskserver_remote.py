"""Module to launch and configure a remote Seamless Dask server."""

from __future__ import annotations

import sys
from typing import Any, Dict

from frozendict import frozendict

from seamless.util.pylru import lrucache

DISABLED = False  # to disable automatic activation during tests

_launcher_cache = lrucache(1000)
_launched_handle: "DaskserverLaunchedHandle | None" = None


def _freeze_mapping(mapping: Dict[str, Any]) -> frozendict:
    """Recursively convert a mapping to a frozendict for cache keys."""

    def _freeze(value: Any) -> Any:
        if isinstance(value, dict):
            return frozendict({k: _freeze(v) for k, v in value.items()})
        if isinstance(value, list):
            return tuple(_freeze(v) for v in value)
        return value

    return frozendict({k: _freeze(v) for k, v in mapping.items()})


class DaskserverLaunchedHandle:
    """Synchronous launcher that yields a SeamlessDaskClient."""

    launch_config: dict
    launch_payload: dict
    client: Any
    cores: int
    dashboard_url: str | None
    remote_clients: dict | None
    interactive: bool

    def __init__(
        self,
        cluster: str,
        project: str | None,
        subproject: str | None,
        stage: str | None,
        substage: str | None,
    ):
        self.dashboard_url = None
        self.remote_clients = None
        self.cores = 1
        self.interactive = False
        self.config(cluster, project, subproject, stage, substage)
        self._do_init()

    def config(
        self,
        cluster: str,
        project: str | None,
        subproject: str | None,
        stage: str | None,
        substage: str | None,
    ) -> None:
        import seamless_config.tools
        from seamless_config import collect_remote_clients

        self.launch_config = seamless_config.tools.configure_daskserver(
            cluster=cluster,
            project=project,
            subproject=subproject,
            stage=stage,
            substage=substage,
        )
        self.remote_clients = collect_remote_clients(cluster)
        file_params = self.launch_config.get("file_parameters") or {}  # type: ignore[arg-type]
        try:
            self.cores = int(file_params.get("cores") or 1)
        except Exception:
            self.cores = 1
        try:
            self.interactive = bool(file_params.get("interactive"))
        except Exception:
            self.interactive = False

    def _do_init(self) -> None:
        import remote_http_launcher

        conf = self.launch_config
        frozenconf = _freeze_mapping(conf)
        payload = _launcher_cache.get(frozenconf)
        if payload is None:
            print("Launch daskserver...", file=sys.stderr)
            payload = remote_http_launcher.run(conf)
            _launcher_cache[frozenconf] = payload

        self.launch_payload = payload
        hostname = payload.get("hostname", "localhost")
        scheduler_port = int(payload["port"])
        dashboard_port = payload.get("dashboard_port")
        if dashboard_port is not None:
            try:
                dashboard_port = int(dashboard_port)
                self.dashboard_url = f"http://{hostname}:{dashboard_port}"
            except Exception:
                self.dashboard_url = None

        scheduler_address = f"tcp://{hostname}:{scheduler_port}"

        from distributed import Client as DistributedClient
        from seamless_dask.client import SeamlessDaskClient

        cluster_string = (self.launch_config or {}).get("cluster_string", "")
        is_local_cluster = "LocalCluster" in str(cluster_string)

        distributed_client = DistributedClient(
            scheduler_address, timeout="10s", set_as_default=False
        )
        self.client = SeamlessDaskClient(
            distributed_client,
            worker_plugin_workers=self.cores,
            remote_clients=self.remote_clients,
            is_local_cluster=is_local_cluster,
            interactive=self.interactive,
        )


def activate(*, no_main: bool = False) -> None:
    """Launch the remote daskserver and configure the Seamless Dask client."""

    global _launched_handle
    if DISABLED or no_main:
        return
    from seamless_config.select import get_current
    from seamless_dask.transformer_client import set_seamless_dask_client

    cluster, project, subproject, stage, substage = get_current()

    _launched_handle = DaskserverLaunchedHandle(
        cluster, project, subproject, stage, substage
    )
    set_seamless_dask_client(_launched_handle.client)
    if _launched_handle.dashboard_url:
        print(f"Dask dashboard: {_launched_handle.dashboard_url}")


def deactivate() -> None:
    """Clear the current Seamless Dask client and launched handle."""

    global _launched_handle
    from seamless_dask.transformer_client import set_seamless_dask_client

    _launched_handle = None
    set_seamless_dask_client(None)


def ensure_initialized():
    # In this case, RAII
    pass


__all__ = ["activate", "deactivate", "DaskserverLaunchedHandle", "ensure_initialized"]
