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


from .client import (
    inspect_launched_clients as _inspect_launched_clients,
    inspect_extern_clients as _inspect_extern_clients,
)


def inspect_launched_clients():
    from .buffer_client import _launcher_cache

    return _inspect_launched_clients(_launcher_cache, _launched_clients)


def inspect_extern_clients():
    return _inspect_extern_clients(_extern_clients)


_read_server_clients: list[BufferClient] = []
_read_folders_clients: list[BufferClient] = []
_write_server_clients: list[BufferClient] = []


def has_write_server() -> bool:
    """Return True when at least one buffer write server client is configured."""

    try:
        return bool(_write_server_clients)
    except Exception:
        return False


def has_read_server() -> bool:
    """Return True when at least one buffer read server client is configured."""

    try:
        return bool(_read_server_clients)
    except Exception:
        return False


# TODO extra launched clients and extern clients in config YAML
def activate(
    *, readonly=False, extra_launched_clients=[], extern_clients=[], no_main=False
):
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


async def get_buffer_lengths(checksums: list[Checksum]) -> list[int | None]:
    """Retrieve buffer lengths from remote sources.
    First all buffer read folders are queried.
    Then all buffer read servers are queried."""

    checksums = list(checksums)
    if not checksums:
        return []

    results: list[int | None] = [None] * len(checksums)
    pending: list[tuple[int, Checksum]] = []
    for idx, checksum in enumerate(checksums):
        checksum = Checksum(checksum)
        if not checksum:
            results[idx] = None
            continue
        pending.append((idx, checksum))

    async def _update_from_client(client, pending_list):
        sub_checksums = [checksum for _, checksum in pending_list]
        lengths = await client.buffer_lengths(sub_checksums)
        if not isinstance(lengths, list) or len(lengths) != len(sub_checksums):
            return pending_list
        new_pending = []
        for (idx, _checksum), length in zip(pending_list, lengths):
            if isinstance(length, int) and length >= 0:
                results[idx] = length
            else:
                new_pending.append((idx, _checksum))
        return new_pending

    for client in _read_folders_clients:
        if not pending:
            return results
        try:
            pending = await _update_from_client(client, pending)
        except Exception:
            continue

    for client in _read_server_clients:
        if not pending:
            return results
        try:
            pending = await _update_from_client(client, pending)
        except Exception:
            continue

    return results


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


async def promise(checksum: Checksum) -> None:
    """Make a promise to the buffer write servers, if one or more exist."""
    checksum = Checksum(checksum)
    """
    if checksum in _written_buffers:
        return
    """
    # print("PROMISE BUFFER", checksum.hex())

    # TODO: do this in parallel
    for client in _write_server_clients:
        await client.promise(checksum)
    return


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
        # DO NOT check for buffer_length / has, because of promises.
        #  If the buffer is present already, the hashserver will return instantly
        ok = await client.write(checksum, buffer)
        if ok:
            written = True
    return written
