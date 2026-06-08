"""Module to access Seamless jobservers."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Dict

from seamless import Checksum

from .client import ClientRestartRequiredError
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
    strict_dunder: bool = False,
):
    if not _jobserver_clients:
        raise RuntimeError("No jobserver clients are available")
    tf_checksum = Checksum(tf_checksum)
    for client in _jobserver_clients:
        for attempt in range(2):
            try:
                try:
                    return await client.run_transformation(
                        transformation_dict,
                        tf_checksum=tf_checksum,
                        tf_dunder=tf_dunder,
                        scratch=scratch,
                        strict_dunder=strict_dunder,
                    )
                except asyncio.CancelledError:
                    with contextlib.suppress(Exception):
                        await client.cancel_transformation(tf_checksum)
                    raise
            except ClientRestartRequiredError:
                client.restart()
                if attempt == 1:
                    raise
    raise RuntimeError("Unreachable")


async def cancel_transformation_async(tf_checksum: Checksum) -> bool:
    if not _jobserver_clients:
        raise RuntimeError("No jobserver clients are available")
    tf_checksum = Checksum(tf_checksum)
    canceled = False
    for client in _jobserver_clients:
        for attempt in range(2):
            try:
                canceled = (
                    bool(await client.cancel_transformation(tf_checksum)) or canceled
                )
                break
            except ClientRestartRequiredError:
                client.restart()
                if attempt == 1:
                    raise
    return canceled


def cancel_transformation(tf_checksum: Checksum) -> bool:
    import asyncio

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        raise RuntimeError("Cannot block on cancel_transformation() in a running loop")
    return asyncio.run(cancel_transformation_async(tf_checksum))


async def transformation_status_async(tf_checksum: Checksum) -> str:
    if not _jobserver_clients:
        raise RuntimeError("No jobserver clients are available")
    tf_checksum = Checksum(tf_checksum)
    for client in _jobserver_clients:
        for attempt in range(2):
            try:
                status = await client.transformation_status(tf_checksum)
                if status != "not-running":
                    return status
                break
            except ClientRestartRequiredError:
                client.restart()
                if attempt == 1:
                    raise
    return "not-running"


def transformation_status(tf_checksum: Checksum) -> str:
    import asyncio

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None and loop.is_running():
        raise RuntimeError("Cannot block on transformation_status() in a running loop")
    return asyncio.run(transformation_status_async(tf_checksum))


def ensure_initialized():
    for client in _launched_clients.values():
        client.ensure_initialized_sync()
