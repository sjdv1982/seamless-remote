"""Small in-process fakes for expression and HashType remote services."""

import sys
from types import ModuleType

from seamless import Buffer, Checksum
from seamless.caching.buffer_cache import get_buffer_cache
from seamless.checksum import expression as expression_mod
from seamless.checksum.cached_calculate_checksum import checksum_cache


def drop_buffer(checksum):
    checksum = Checksum(checksum)
    cache = get_buffer_cache()
    with cache.lock:
        cache.weak_cache.pop(checksum, None)
        cache.strong_cache.pop(checksum, None)
    checksum_cache.pop(checksum, None)
    expression_mod._expression_result_buffers.pop(checksum, None)


def install_fake_remotes(
    monkeypatch,
    expression_rows,
    jobserver_results,
    calls,
    *,
    buffers=None,
    hash_type_rows=None,
    jobserver_available=True,
    run_expression_gate=None,
    run_expression_started=None,
):
    from seamless.caching import buffer_writer
    buffer_writer.flush()
    if buffers is None:
        buffers = {}
    if hash_type_rows is None:
        hash_type_rows = {}
    seamless_remote = ModuleType("seamless_remote")
    seamless_remote.__path__ = []
    database_remote = ModuleType("seamless_remote.database_remote")
    jobserver_remote = ModuleType("seamless_remote.jobserver_remote")
    buffer_remote = ModuleType("seamless_remote.buffer_remote")
    buffer_remote._read_folders_clients = []

    def key(input_checksum, path, celltype, target_celltype):
        return (
            Checksum(input_checksum).hex(),
            path,
            celltype,
            target_celltype,
        )

    async def get_expression_result(input_checksum, path, celltype, target_celltype):
        calls.append("database:get")
        return expression_rows.get(key(input_checksum, path, celltype, target_celltype))

    async def set_expression_result(
        input_checksum, path, celltype, target_celltype, result_checksum
    ):
        calls.append("database:set")
        expression_rows[key(input_checksum, path, celltype, target_celltype)] = Checksum(
            result_checksum
        )
        return True

    async def get_hash_type(checksum):
        calls.append("database:get_hash_type")
        return hash_type_rows.get(Checksum(checksum).hex())

    async def set_hash_type(checksum, hash_type):
        calls.append("database:set_hash_type")
        hash_type_rows[Checksum(checksum).hex()] = int(hash_type)
        return True

    async def run_expression(input_checksum, path, celltype, target_celltype, *, scratch=False):
        if not jobserver_available:
            raise RuntimeError("No jobserver clients are available")
        calls.append("jobserver:run")
        if run_expression_started is not None:
            run_expression_started.set()
        if run_expression_gate is not None:
            wait = run_expression_gate.wait()
            if hasattr(wait, "__await__"):
                await wait
        return Checksum(jobserver_results[key(input_checksum, path, celltype, target_celltype)])

    database_remote.get_expression_result = get_expression_result
    database_remote.set_expression_result = set_expression_result
    database_remote.get_hash_type = get_hash_type
    database_remote.set_hash_type = set_hash_type
    jobserver_remote.run_expression = run_expression
    jobserver_remote.has_jobserver = lambda: jobserver_available
    jobserver_remote._jobserver_clients = [object()] if jobserver_available else []

    async def get_buffer(checksum):
        calls.append("hashserver:get")
        checksum = Checksum(checksum)
        content = buffers.get(checksum)
        if content is None:
            return None
        return Buffer(content, checksum=checksum)

    buffer_remote.get_buffer = get_buffer
    seamless_remote.database_remote = database_remote
    seamless_remote.jobserver_remote = jobserver_remote
    seamless_remote.buffer_remote = buffer_remote
    monkeypatch.setitem(sys.modules, "seamless_remote", seamless_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.database_remote", database_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.jobserver_remote", jobserver_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.buffer_remote", buffer_remote)


_drop_buffer = drop_buffer
_install_fake_remotes = install_fake_remotes
