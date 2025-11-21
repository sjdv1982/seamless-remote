"""Module to read and write to Seamless databases
Primarily for transformation results, but also:
- conversions
- syntactic <=> semantic code  (source <=> AST)
- ...
"""

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


_read_database_clients: list[DatabaseClient] = []
_write_database_clients: list[DatabaseClient] = []


# TODO extra launched clients and extern clients in config YAML
def activate(*, readonly=False, extra_launched_clients=[], extern_clients=[]):
    if DISABLED:
        return
    from seamless_config.select import get_current

    cluster, project, subproject, stage, _ = get_current()
    assert cluster is not None and project is not None

    rclients = []
    wclients = []

    launch_keys = []

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
        result = await client.get_transformation_result(tf_checksum)
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
