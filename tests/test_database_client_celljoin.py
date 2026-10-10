"""Celljoin database wire shapes, decoding, errors and multi-client routing."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from aiohttp import ClientConnectionError
from seamless import Checksum
from seamless_remote.database_client import DatabaseClient
from seamless_remote import database_remote

JOIN = Checksum('a' * 64)
RESULT = Checksum('b' * 64)

class Response:
    def __init__(self, status=200, text='OK'):
        self.status, self.body = status, text
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        return False
    async def text(self):
        return self.body
    async def json(self):
        return json.loads(self.body)

@pytest.fixture
def client(monkeypatch):
    client = DatabaseClient(readonly=False)
    client.url = 'http://database.invalid'
    client._initialized = True
    calls = []
    state = [Response()]
    def request(url, *, json=None, timeout=None):
        if url.endswith('/healthcheck'):
            assert json is None
            return Response()
        calls.append(json)
        return state[0]
    monkeypatch.setattr(client, '_get_session', lambda: SimpleNamespace(get=request, put=request))
    monkeypatch.setattr(client, '_get_semaphore', lambda: None)
    return client, calls, state

def test_wire_identity_and_reverse_checksum_decoding(client):
    db, calls, state = client
    asyncio.run(db.set_celljoin_result(JOIN, 'plain', RESULT))
    assert calls[-1] == {'type': 'celljoin', 'checksum': JOIN.hex(), 'celltype': 'plain', 'value': RESULT.hex()}
    state[0] = Response(text=RESULT.hex())
    assert asyncio.run(db.get_celljoin_result(JOIN, 'plain')) == RESULT
    assert calls[-1] == {'type': 'celljoin', 'checksum': JOIN.hex(), 'celltype': 'plain'}
    state[0] = Response(text=json.dumps([{'checksum': JOIN.hex(), 'celltype': 'mixed'}]))
    assert asyncio.run(db.get_rev_celljoins(RESULT)) == [{'checksum': JOIN, 'celltype': 'mixed'}]
    assert calls[-1] == {'type': 'rev_celljoins', 'checksum': RESULT.hex()}

@pytest.mark.parametrize('method,args', [('get_celljoin_result', (JOIN, 'plain')), ('get_rev_celljoins', (RESULT,))])
def test_missing_records_return_none(client, method, args):
    db, _, state = client
    state[0] = Response(404, 'Unknown')
    assert asyncio.run(getattr(db, method)(*args)) is None

def test_conflict_is_nonfatal_but_other_http_errors_propagate(client):
    db, calls, state = client
    state[0] = Response(409, 'CellJoin already exists with different result')
    assert asyncio.run(db.set_celljoin_result(JOIN, 'plain', RESULT)) is False
    state[0] = Response(400, 'bad celltype')
    with pytest.raises(ClientConnectionError):
        asyncio.run(db.set_celljoin_result(JOIN, 'plain', RESULT))
    # The public client retries ordinary HTTP errors after healthy reconnects.
    assert len(calls) == 6  # one nonfatal conflict, then five failed attempts

def test_readonly_client_refuses_write_without_request(client):
    db, calls, _ = client
    db.readonly = True
    with pytest.raises(AttributeError):
        asyncio.run(db.set_celljoin_result(JOIN, 'plain', RESULT))
    assert calls == []

def test_remote_reads_next_client_and_writes_every_client(monkeypatch):
    first = SimpleNamespace(get_celljoin_result=AsyncMock(return_value=None), get_rev_celljoins=AsyncMock(return_value=None), set_celljoin_result=AsyncMock(return_value=False))
    rows = [{'checksum': JOIN, 'celltype': 'plain'}]
    second = SimpleNamespace(get_celljoin_result=AsyncMock(return_value=RESULT), get_rev_celljoins=AsyncMock(return_value=rows), set_celljoin_result=AsyncMock(return_value=None))
    monkeypatch.setattr(database_remote, '_read_database_clients', [first, second])
    monkeypatch.setattr(database_remote, '_write_database_clients', [first, second])
    assert asyncio.run(database_remote.get_celljoin_result(JOIN, 'plain')) == RESULT
    assert asyncio.run(database_remote.get_rev_celljoins(RESULT)) == rows
    assert asyncio.run(database_remote.set_celljoin_result(JOIN, 'plain', RESULT)) is True
    for client in (first, second):
        client.get_celljoin_result.assert_awaited_once_with(JOIN, 'plain')
        client.get_rev_celljoins.assert_awaited_once_with(RESULT)
        client.set_celljoin_result.assert_awaited_once_with(JOIN, 'plain', RESULT)
