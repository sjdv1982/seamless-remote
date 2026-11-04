"""Configured local folders participate before a hashserver is launched."""

import asyncio

import seamless
import seamless_config.select
import seamless_config.tools
from seamless import Checksum
from seamless.caching.buffer_source import has_buffer_source, unregister_buffer_sources
from seamless.checksum.calculate_checksum import calculate_checksum
from seamless.checksum.expression import choose_expression_evaluation_location
from seamless_remote import buffer_remote
from seamless_remote.buffer_client import BufferLaunchedClient


def test_local_launched_folder_is_available_without_launch_and_removed_on_reconfigure(
    tmp_path, monkeypatch,
):
    content = b'{"value": 42, "test": "lazy configured folder"}'
    checksum = Checksum(calculate_checksum(content))
    (tmp_path / checksum.hex()).write_bytes(content)
    monkeypatch.setattr(
        seamless_config.tools, 'configure_hashserver',
        lambda **kwargs: {'workdir': str(tmp_path)},
    )
    monkeypatch.setattr(
        seamless_config.select, 'get_current',
        lambda *args: ('test-cluster', 'test-project', '', '', None),
    )
    client = BufferLaunchedClient(readonly=True)
    client.config('test-cluster', 'test-project', '', '')

    def forbidden_launch():
        raise AssertionError('Reading an existing local buffer must not launch a server')

    monkeypatch.setattr(client, '_do_init', forbidden_launch)
    monkeypatch.setattr(buffer_remote, 'DISABLED', False)
    monkeypatch.setattr(buffer_remote, '_launched_clients', {
        (True, 'test-cluster', 'test-project', '', ''): client,
    })
    monkeypatch.setattr(buffer_remote, '_read_folders_clients', [])
    monkeypatch.setattr(buffer_remote, '_read_server_clients', [])
    monkeypatch.setattr(buffer_remote, '_write_server_clients', [])

    try:
        buffer_remote.activate(readonly=True)
        assert has_buffer_source(checksum)
        assert choose_expression_evaluation_location(
            checksum, 'value', 'plain', 'int',
        ) == 'local'
        assert checksum.resolve().content == content
        assert asyncio.run(buffer_remote.get_buffer(checksum)).content == content

        monkeypatch.setattr(
            seamless_config.tools, 'configure_hashserver',
            lambda **kwargs: {'hostname': 'remote-host', 'workdir': '/remote/buffers'},
        )
        client.config('test-cluster', 'test-project', '', '')
        assert client.directory is None
        assert not has_buffer_source(checksum)

        buffer_remote.activate(readonly=True, no_main=True)
        assert not has_buffer_source(checksum)
    finally:
        unregister_buffer_sources(buffer_remote._read_folder_source_owner)
