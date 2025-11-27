"""Module to obtain buffers from remote sources:
- Buffer read servers
- Buffer read folders
and to write to them
- Buffer write servers
"""

import traceback
from seamless import Buffer, Checksum
from .buffer_client import BufferClient, BufferLaunchedClient
import aiofiles.os

DISABLED = False  # to disable automatic activation during tests

_launched_clients: dict[tuple, BufferLaunchedClient] = {}
_extern_clients: dict[str, BufferClient] = {}


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
    client = BufferLaunchedClient(readonly)
    client.config(cluster, project, subproject, stage)
    _launched_clients[key] = client


def define_extern_client(
    name, type_, *, params=None, directory=None, url=None, readonly=True
):
    if type_ == "hashserver":
        assert url is not None and directory is None
        client = BufferClient(readonly)
        client.url = url
    elif type_ == "bufferfolder":
        assert url is None and directory is not None
        assert readonly
        client = BufferClient(True)
        client.directory = directory
    elif type_ == "urlmap":
        raise NotImplementedError
    elif type_ == "foldermap":
        raise NotImplementedError
    elif type_ == "git-lfs-server":
        raise NotImplementedError
    else:
        raise TypeError(type_)
    _extern_clients[name] = client


def inspect_extern_clients():
    """Return inspection info for all defined external buffer clients."""
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
                "directory": getattr(client, "directory", None),
            }
        )
    return result


def inspect_launched_clients():
    """Return inspection info for all launched buffer clients."""
    result = []
    from frozendict import frozendict
    from .buffer_client import _launcher_cache

    for key, client in _launched_clients.items():
        readonly, cluster, project, subproject, stage = key
        try:
            client.ensure_initialized_sync(skip_healthcheck=True)
        except Exception:
            import traceback

            traceback.print_exc()
            pass
        launch_conf = getattr(client, "launch_config", {}) or {}
        server_conf = _launcher_cache.get(frozendict(launch_conf))
        tun_host = None
        tun_port = None
        if server_conf is not None:
            tun_host = server_conf.get("tunneled-network-interface")
            tun_port = server_conf.get("tunneled-port")
        tun_url = None
        if tun_host is not None and tun_port is not None:
            try:
                tun_url = f"http://{tun_host}:{int(tun_port)}"
            except Exception:
                tun_url = None
        result.append(
            {
                "readonly": bool(readonly),
                "cluster": cluster,
                "project": project,
                "subproject": subproject,
                "stage": stage,
                "url": tun_url if tun_url is not None else getattr(client, "url", None),
                "directory": getattr(client, "directory", None),
            }
        )
    return result


_read_server_clients: list[BufferClient] = []
_read_folders_clients: list[BufferClient] = []
_write_server_clients: list[BufferClient] = []


# TODO extra launched clients and extern clients in config YAML
def activate(*, readonly=False, extra_launched_clients=[], extern_clients=[], no_main=False):
    if DISABLED:
        return
    from seamless_config.select import get_current

    rs_clients = []
    rf_clients = []
    ws_clients = []
    launch_keys = []

    if not no_main:
        cluster, project, subproject, stage, _ = get_current()
        assert cluster is not None and project is not None

        main_key = readonly, cluster, project, subproject, stage
        launch_keys.append(main_key)
        if main_key not in _launched_clients:
            define_launched_client(*main_key)

        client = _launched_clients[main_key]
        rs_clients.append(client)
        if client.local:
            rf_clients.append(client)
        if not readonly:
            ws_clients.append(_launched_clients[main_key])

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
        rs_clients.append(client)
        if client.local:
            rf_clients.append(client)
        if not readonly:
            ws_clients.append(client)

    for name in extern_clients:
        if name not in _extern_clients:
            raise RuntimeError(f"Unknown extern client '{name}'")
        client = _extern_clients[name]
        if client.directory:
            rf_clients.append(client)
        if client.url:
            rs_clients.append(client)
            if not client.readonly:
                ws_clients.append(client)

    _read_server_clients[:] = rs_clients
    _read_folders_clients[:] = rf_clients
    _write_server_clients[:] = ws_clients


"""
_known_buffers = set()
_written_buffers = set()
"""


async def get_buffer(checksum: Checksum) -> Buffer | None:
    """Retrieve the buffer from remote sources.
    First all buffer read folders are queried.
    Then all buffer read servers are queried."""

    checksum = Checksum(checksum)

    for client in _read_folders_clients:
        buf = await client.get_file_buffer(checksum)
        if buf is not None:
            return buf

    for client in _read_server_clients:
        buf = await client.get(checksum)
        if buf is not None:
            return buf


async def get_filename(checksum: Checksum) -> str | None:
    """Get the filename where a buffer is stored.
    This can only be provided by a buffer read folder.
    All buffer read folders are queried in order."""
    raise NotImplementedError

    """
    checksum = Checksum(checksum)
    if not checksum:
        return None
    if _read_folders is None:
        return None
    for folder in _read_folders:
        filename = aiofiles.os.path.join(folder, checksum.hex())
        if aiofiles.os.path.exists(filename):
            return filename
    return None
    """


async def write_buffer(checksum: Checksum, buffer: Buffer) -> bool:
    """Write the buffer to the buffer write servers, if one or more exist."""
    checksum = Checksum(checksum)
    """
    if checksum in _written_buffers:
        return
    _written_buffers.add(checksum)
    """
    # print("WRITE BUFFER", checksum.hex())

    # TODO: do this in parallel
    written = False
    for client in _write_server_clients:
        if await client.buffer_length(checksum):
            continue
        ok = await client.write(checksum, buffer)
        if ok:
            written = True
    return written
