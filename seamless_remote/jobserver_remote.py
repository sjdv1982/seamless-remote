"""Module to access Seamless jobservers."""

from __future__ import annotations

from typing import Any, Dict

from seamless import Checksum

from .jobserver_client import JobserverClient, JobserverLaunchedClient

DISABLED = False  # to disable automatic activation during tests

_launched_clients: dict[tuple, JobserverLaunchedClient] = {}
_extern_clients: dict[str, JobserverClient] = {}
_jobserver_clients: list[JobserverClient] = []


def define_launched_client(
    cluster: str | None,
    project: str | None,
    subproject: str | None = "",
    stage: str | None = "",
    substage: str | None = "",
):
    from seamless_config.select import get_current

    cluster, project, subproject, stage, substage = get_current(
        cluster, project, subproject, stage, substage
    )

    key = cluster, project, subproject, stage, substage
    client = JobserverLaunchedClient()
    client.config(cluster, project, subproject, stage, substage)
    _launched_clients[key] = client


def define_extern_client(name, type_, *, url=None):
    if type_ == "jobserver":
        assert url is not None
        client = JobserverClient()
        client.url = url
    else:
        raise TypeError(type_)
    _extern_clients[name] = client


def activate(
    *,
    extra_launched_clients: list[dict] | None = None,
    extern_clients: list[str] | None = None,
    no_main: bool = False,
):
    if DISABLED:
        return
    from seamless_config.select import get_current

    if extra_launched_clients is None:
        extra_launched_clients = []
    if extern_clients is None:
        extern_clients = []

    clients = []
    launch_keys = []

    cluster = project = subproject = stage = substage = None
    if not no_main:
        cluster, project, subproject, stage, substage = get_current()
        main_key = cluster, project, subproject, stage, substage
        launch_keys.append(main_key)
        if main_key not in _launched_clients:
            define_launched_client(*main_key)
        clients.append(_launched_clients[main_key])

    for params in extra_launched_clients:
        c_cluster = params.get("cluster", cluster)
        c_project = params.get("project", project)
        c_subproject = params.get("subproject", subproject)
        c_stage = params.get("stage", stage)
        c_substage = params.get("substage", substage)
        k = c_cluster, c_project, c_subproject, c_stage, c_substage
        if k in launch_keys:
            raise RuntimeError("Redundant extra launched client:" + str(params))
        if k in launch_keys:
            continue
        if k not in _launched_clients:
            define_launched_client(*k)
        client = _launched_clients[k]
        clients.append(client)

    for name in extern_clients:
        if name not in _extern_clients:
            raise RuntimeError(f"Unknown extern client '{name}'")
        clients.append(_extern_clients[name])

    for client in clients:
        if isinstance(client, JobserverLaunchedClient):
            client.ensure_initialized_sync(skip_healthcheck=False)

    _jobserver_clients[:] = clients


async def run_transformation(
    transformation_dict: Dict[str, Any],
    *,
    tf_checksum: Checksum,
    tf_dunder: Dict[str, Any],
    scratch: bool,
):
    if not _jobserver_clients:
        raise RuntimeError("No jobserver clients are available")
    tf_checksum = Checksum(tf_checksum)
    for client in _jobserver_clients:
        return await client.run_transformation(
            transformation_dict,
            tf_checksum=tf_checksum,
            tf_dunder=tf_dunder,
            scratch=scratch,
        )
    raise RuntimeError("Unreachable")


def ensure_initialized():
    for client in _launched_clients.values():
        client.ensure_initialized_sync()
