import asyncio
import json
from contextlib import AbstractAsyncContextManager

import pytest
from seamless_remote.jobserver_client import JobserverClient


class Response(AbstractAsyncContextManager):
    status = 200
    def __init__(self, body):
        self.body = body
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        return False
    async def text(self):
        return json.dumps(self.body)


class Session:
    def __init__(self, body):
        self.body = body
    def get(self, *args, **kwargs):
        return Response(self.body)


@pytest.mark.parametrize('include_logs', [True, False])
def test_client_passes_logs_and_tolerates_old_response(include_logs):
    body = {'result_checksum': 'b' * 64, 'record_runtime': {'wall_time_seconds': 2.5}}
    logs = {'stdout': 'remote out\n', 'stderr': 'remote err\n'}
    if include_logs:
        body['logs'] = logs
    client = JobserverClient()
    client.url = 'http://jobserver.invalid'
    client._initialized = True
    client._get_session = lambda: Session(body)
    reply = asyncio.run(client.run_transformation({'__language__': 'python'}, tf_checksum='a' * 64, tf_dunder={}, scratch=False))
    assert reply['result_checksum'].hex() == 'b' * 64
    assert reply['record_runtime']['wall_time_seconds'] == 2.5
    assert reply.get('logs') == (logs if include_logs else None)
