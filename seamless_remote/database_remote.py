"""Module to read and write to Seamless databases
Primarily for transformation results, but also:
- conversions
- syntactic <=> semantic code  (source <=> AST)
- ...
"""

import os

from seamless import Buffer, Checksum
from .database_client import DatabaseClient, DatabaseLaunchedClient

DISABLED = False  # to disable automatic activation during tests

_launched_clients: dict[tuple, DatabaseLaunchedClient] = {}
_extern_clients: dict[str, DatabaseClient] = {}


def define_launched_client(
    readonly: bool,
    cluster: str | None,
    project: str | None,
    subproject: str | None = "",
    stage: str | None = "",
):
    from seamless_config.select import get_current

    cluster, project, subproject, stage, _ = get_current(
        cluster, project, subproject, stage
    )

    key = readonly, cluster, project, subproject, stage
    client = DatabaseLaunchedClient(readonly)
    client.config(cluster, project, subproject, stage)
    _launched_clients[key] = client


def define_extern_client(name, type_, *, params=None, url=None, readonly=True):
    if type_ == "database":
        assert url is not None
        client = DatabaseClient(readonly)
        client.url = url
    else:
        raise TypeError(type_)
    _extern_clients[name] = client


def inspect_extern_clients():
    """Return inspection info for all defined external database clients."""
    result = []
    for name, client in _extern_clients.items():
        try:
            client.ensure_initialized_sync(skip_healthcheck=True)
        except Exception:
            pass
        result.append(
            {
                "name": name,
                "readonly": bool(client.readonly),
                "url": getattr(client, "url", None),
            }
        )
    return result


def inspect_launched_clients():
    """Return inspection info for all launched database clients."""
    from frozendict import frozendict
    from .database_client import _launcher_cache

    result = []

    def _ensure_scheme(url: str | None) -> str | None:
        if url is None:
            return None
        if "://" in url:
            return url
        return "http://" + url

    for key, client in _launched_clients.items():
        readonly, cluster, project, subproject, stage = key
        try:
            client.ensure_initialized_sync(skip_healthcheck=True)
        except Exception:
            pass

        launch_conf = getattr(client, "launch_config", {}) or {}
        server_conf = _launcher_cache.get(frozendict(launch_conf))
        tun_host = None
        tun_port = None
        if server_conf is not None:
            tun_host = server_conf.get("tunneled-network-interface")
            tun_port = server_conf.get("tunneled-port")
        remote_url = None
        if server_conf is not None:
            remote_host = launch_conf.get("hostname") or server_conf.get(
                "tunneled-network-interface"
            )
            remote_port = server_conf.get("tunneled-port") or server_conf.get("port")
            if remote_host is not None and remote_port is not None:
                try:
                    remote_url = f"http://{remote_host}:{int(remote_port)}"
                except Exception:
                    remote_url = None
        tun_url = None
        if tun_host is not None and tun_port is not None:
            try:
                tun_url = f"http://{tun_host}:{int(tun_port)}"
            except Exception:
                tun_url = None

        url = tun_url if tun_url is not None else getattr(client, "url", None)
        result.append(
            {
                "readonly": bool(readonly),
                "cluster": cluster,
                "project": project,
                "subproject": subproject,
                "stage": stage,
                "url": _ensure_scheme(url),
                "remote_url": _ensure_scheme(remote_url),
            }
        )
    return result


_read_database_clients: list[DatabaseClient] = []
_write_database_clients: list[DatabaseClient] = []
_DEBUG = os.environ.get("SEAMLESS_DEBUG_REMOTE_DB", "").lower() in ("1", "true", "yes")


def has_write_server() -> bool:
    """Return True when at least one database write client is configured."""

    try:
        return bool(_write_database_clients)
    except Exception:
        return False


def has_read_server() -> bool:
    """Return True when at least one database read client is configured."""

    try:
        return bool(_read_database_clients)
    except Exception:
        return False


def _debug(msg: str) -> None:
    if _DEBUG:
        print(f"[database_remote] {msg}", flush=True)


# TODO extra launched clients and extern clients in config YAML
def activate(
    *, readonly=False, extra_launched_clients=[], extern_clients=[], no_main=False
):
    if DISABLED:
        return
    from seamless_config.select import get_current

    rclients = []
    wclients = []

    launch_keys = []

    if not no_main:
        cluster, project, subproject, stage, _ = get_current()
        assert cluster is not None and project is not None

        main_key = readonly, cluster, project, subproject, stage
        launch_keys.append(main_key)
        if main_key not in _launched_clients:
            define_launched_client(*main_key)

        client = _launched_clients[main_key]
        rclients.append(client)
        if not readonly:
            wclients.append(_launched_clients[main_key])

    for params in extra_launched_clients:
        c_readonly = params.get("readonly", True)
        c_cluster = params.get("cluster", cluster)
        c_project = params.get("project", project)
        c_subproject = params.get("subproject", subproject)
        c_stage = params.get("stage", stage)
        k = c_readonly, c_cluster, c_project, c_subproject, c_stage
        k2 = False, c_cluster, c_project, c_subproject, c_stage
        if k in launch_keys:
            raise RuntimeError("Redundant extra launched client:" + str(params))
        if k in launch_keys or k2 in launch_keys:
            continue
        if k not in _launched_clients:
            define_launched_client(*k)
        client = _launched_clients[k]
        rclients.append(client)
        if not readonly:
            wclients.append(client)

    for name in extern_clients:
        if name not in _extern_clients:
            raise RuntimeError(f"Unknown extern client '{name}'")
        client = _extern_clients[name]
        rclients.append(client)
        if not client.readonly:
            wclients.append(client)

    _read_database_clients[:] = rclients
    _write_database_clients[:] = wclients


async def get_transformation_result(tf_checksum: Checksum) -> Checksum | None:
    """Return result of the transformation 'tf_checksum' from remote databases, if known."""

    tf_checksum = Checksum(tf_checksum)

    for client in _read_database_clients:
        _debug(f"query {client} for {tf_checksum.hex()}")
        result = await client.get_transformation_result(tf_checksum)
        _debug(f"client {client} returned {result}")
        if result is not None:
            return result


async def set_transformation_result(tf_checksum: Checksum, result_checksum: Checksum):
    """Write the transformation result to remote databases"""
    tf_checksum = Checksum(tf_checksum)
    written = False
    for client in _write_database_clients:
        ok = await client.set_transformation_result(tf_checksum, result_checksum)
        if ok:
            written = True
    return written
