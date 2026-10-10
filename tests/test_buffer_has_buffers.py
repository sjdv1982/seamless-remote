"""Server-only, batched buffer presence for celljoin placement."""
import asyncio

from seamless import Checksum
from seamless_remote import buffer_remote


class Server:
    def __init__(self, answers, *, url='http://hashserver.invalid'):
        self.url = url
        self.answers = answers
        self.calls = []

    async def buffer_lengths(self, checksums):
        self.calls.append(tuple(checksums))
        return [self.answers.get(cs) for cs in checksums]


def configure(monkeypatch, servers, folders=()):
    monkeypatch.setattr(buffer_remote, '_read_server_clients', servers)
    monkeypatch.setattr(buffer_remote, '_read_folders_clients', list(folders))


def test_presence_ors_servers_and_stops_when_complete(monkeypatch):
    checksums = [Checksum(f'{i:064x}') for i in range(1, 4)]
    first = Server({checksums[0]: 10})
    second = Server({checksums[1]: True, checksums[2]: 20})
    unused = Server(dict.fromkeys(checksums, 99))
    configure(monkeypatch, [first, second, unused])
    assert asyncio.run(buffer_remote.has_buffers(checksums)) == [True, True, True]
    assert len(first.calls) == len(second.calls) == 1
    assert unused.calls == []


def test_presence_chunks_at_ten_thousand(monkeypatch):
    checksums = [Checksum(f'{i:064x}') for i in range(1, 10002)]
    server = Server(dict.fromkeys(checksums, 1))
    configure(monkeypatch, [server])
    assert asyncio.run(buffer_remote.has_buffers(checksums)) == [True] * len(checksums)
    assert [len(call) for call in server.calls] == [10000, 1]
    assert [cs for call in server.calls for cs in call] == checksums


def test_read_folders_and_clients_without_url_are_excluded(monkeypatch):
    checksum = Checksum('12' * 32)
    folder = Server({checksum: 8}, url=None)
    configure(monkeypatch, [folder], [folder])
    assert asyncio.run(buffer_remote.has_buffers([checksum])) == [False]
    assert folder.calls == []


def test_false_is_absent_but_zero_length_is_present(monkeypatch):
    checksums = [Checksum(f'{i:064x}') for i in range(1, 6)]
    server = Server(dict(zip(checksums, [False, 0, True, None, 8])))
    configure(monkeypatch, [server])
    result = asyncio.run(buffer_remote.has_buffers(checksums))
    assert result == [False, True, True, False, True]
    assert all(type(value) is bool for value in result)


def test_no_servers_and_empty_request(monkeypatch):
    configure(monkeypatch, [])
    assert asyncio.run(buffer_remote.has_buffers([Checksum('34' * 32)])) == [False]
    server = Server({})
    configure(monkeypatch, [server])
    assert asyncio.run(buffer_remote.has_buffers([])) == []
    assert server.calls == []
