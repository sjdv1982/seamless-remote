"""Celljoin jobserver identity wire and structured error propagation."""
import asyncio
import json
from types import SimpleNamespace
import pytest
from seamless import Checksum, CacheMissError
from seamless.error_envelope import encode_error
from seamless_remote.jobserver_client import JobserverClient


class Response:
    status = 200
    def __init__(self, payload):
        self.payload = payload
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        return False
    async def text(self):
        return json.dumps(self.payload)


@pytest.fixture
def subject(monkeypatch):
    subject = JobserverClient()
    subject.url = 'http://jobserver.invalid'
    subject._initialized = True
    state = [{'result_checksum': 'ab' * 32}]
    calls = []
    def get(url, *, json):
        calls.append((url, json))
        return Response(state[0])
    monkeypatch.setattr(subject, '_get_session', lambda: SimpleNamespace(get=get))
    return subject, state, calls


@pytest.mark.parametrize('scratch', [True, False])
def test_identity_only_wire_and_result(subject, scratch):
    client, state, calls = subject
    checksum = Checksum('12' * 32)
    assert asyncio.run(client.run_celljoin(checksum, 'plain', scratch=scratch)) == Checksum('ab' * 32)
    assert calls == [('http://jobserver.invalid/run-celljoin',
        {'celljoin_checksum': checksum.hex(), 'celltype': 'plain', 'scratch': scratch})]


def test_definition_cache_miss_envelope_is_raised(subject):
    client, state, calls = subject
    checksum = Checksum('13' * 32)
    state[0] = encode_error(CacheMissError(checksum))
    with pytest.raises(CacheMissError) as error:
        asyncio.run(client.run_celljoin(checksum, 'plain'))
    assert checksum.hex() in str(error.value)
    assert len(calls) == 1
