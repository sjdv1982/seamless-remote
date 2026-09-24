"""Integration tests for seamless-core behavior with seamless-remote clients."""

import asyncio
import hashlib
from pathlib import Path
import tempfile

import pytest

from seamless import Buffer, Checksum
from seamless.checksum.expression import (
    ExpressionKey,
    evaluate_expression_async,
    evaluate_expression_remote,
    get_expression_cache,
)


def _raise_if_fetched():
    raise AssertionError("this conversion must not fetch its source buffer")


def test_evaluate_expression_async_reads_from_local_buffer_directory(monkeypatch):
    from seamless.checksum import expression
    from seamless.checksum.hash_type import register_hash_type_for_buffer
    from seamless_remote import buffer_remote
    from seamless_remote.buffer_client import BufferClient

    raw = b'"async-buffer-directory-witness"\n'
    checksum = Checksum(hashlib.sha256(raw).digest())
    register_hash_type_for_buffer(checksum, raw)
    key = ExpressionKey(checksum, "", "plain", "str")
    get_expression_cache().clear()

    monkeypatch.setattr(
        expression,
        "_get_local_buffer",
        lambda *args, **kwargs: _raise_if_fetched(),
    )
    monkeypatch.setattr(buffer_remote, "_read_server_clients", [])

    with tempfile.TemporaryDirectory() as directory:
        buffer_path = Path(directory) / checksum.hex()
        buffer_path.write_bytes(raw)
        client = BufferClient(readonly=True)
        client.directory = directory
        reads = []
        original_get_file_buffer = client.get_file_buffer

        async def get_file_buffer(requested_checksum, *args, **kwargs):
            reads.append(requested_checksum)
            return await original_get_file_buffer(
                requested_checksum, *args, **kwargs
            )

        monkeypatch.setattr(client, "get_file_buffer", get_file_buffer)
        monkeypatch.setattr(buffer_remote, "_read_folders_clients", [client])

        result = asyncio.run(
            evaluate_expression_async(
                key.input_checksum,
                key.path,
                key.input_celltype,
                key.celltype,
            )
        )
        assert buffer_path.is_file()
        assert reads == [checksum]
        assert result == checksum
        assert result.resolve("str") == "async-buffer-directory-witness"

    assert not Path(directory).exists()


def test_auto_placement_treats_a_configured_read_folder_as_local(monkeypatch):
    from seamless.caching.buffer_cache import get_buffer_cache
    from seamless.checksum.cached_calculate_checksum import checksum_cache
    from seamless_remote import buffer_remote, jobserver_remote

    source = Buffer({"value": "from folder"}, "plain")
    source_checksum = source.get_checksum()
    cache = get_buffer_cache()
    with cache.lock:
        cache.weak_cache.pop(source_checksum, None)
        cache.strong_cache.pop(source_checksum, None)
    checksum_cache.pop(source_checksum, None)

    class ReadFolder:
        async def get_file_buffer(self, checksum):
            assert checksum == source_checksum
            return source

    async def forbidden_dispatch(*args, **kwargs):
        pytest.fail("an explicitly configured local read folder must not dispatch")

    monkeypatch.setattr(buffer_remote, "_read_folders_clients", [ReadFolder()])
    monkeypatch.setattr(buffer_remote, "_read_server_clients", [])
    monkeypatch.setattr(jobserver_remote, "has_jobserver", lambda: True)
    monkeypatch.setattr(jobserver_remote, "run_expression", forbidden_dispatch)
    get_expression_cache().clear()

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "value",
            "plain",
            "str",
            execution="auto",
        )
    )
    assert result == Buffer("from folder", "str").get_checksum()
