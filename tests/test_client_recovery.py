import asyncio
import unittest
from contextlib import AbstractAsyncContextManager
import sys
from types import ModuleType

from aiohttp import ClientConnectionError

_seamless_transformer = ModuleType("seamless_transformer")
_seamless_transformer.__path__ = []
_remote_job = ModuleType("seamless_transformer.remote_job")
_remote_job.parse_remote_job_written = lambda value: None
_seamless_transformer.remote_job = _remote_job
sys.modules.setdefault("seamless_transformer", _seamless_transformer)
sys.modules.setdefault("seamless_transformer.remote_job", _remote_job)

from seamless_remote import jobserver_remote
from seamless_remote.client import (
    Client,
    ClientRestartRequiredError,
    _retry_operation,
)


class _Response(AbstractAsyncContextManager):
    def __init__(self, *, status=200, text="OK"):
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text


class _BusyThenHealthySession:
    def __init__(self):
        self.calls = 0
        self.closed = False
        self._loop = None

    def get(self, path, timeout=None):
        del timeout
        self.calls += 1
        if self.calls == 1:
            raise ClientConnectionError(
                f"Cannot connect to host {path}: [Device or resource busy]"
            )
        return _Response()


class _HealthcheckClient(Client):
    def __init__(self):
        super().__init__(readonly=True)
        self.url = "http://example.invalid"
        self._session = _BusyThenHealthySession()
        self.restart_count = 0

    async def _init(self):
        return None

    def _validate_init(self):
        return None

    def _get_session(self):
        return self._session

    def restart(self):
        self.restart_count += 1
        self._initialized = False

    @_retry_operation
    async def fetch(self):
        return "ok"


class ClientRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_busy_healthcheck_resets_and_retries(self):
        client = _HealthcheckClient()
        result = await client.fetch()
        self.assertEqual(result, "ok")
        self.assertEqual(client.restart_count, 1)
        self.assertEqual(client._session.calls, 2)

    async def test_busy_healthcheck_raises_restartable_error(self):
        client = _HealthcheckClient()
        with self.assertRaises(ClientRestartRequiredError):
            await client.healthcheck()


class _FakeJobserverClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.cancel_calls = []
        self.restart_calls = 0

    async def run_transformation(self, transformation_dict, **kwargs):
        del transformation_dict, kwargs
        self.calls += 1
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def cancel_transformation(self, tf_checksum):
        self.cancel_calls.append(str(tf_checksum))
        return True

    def restart(self):
        self.restart_calls += 1


class JobserverRemoteRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._saved_clients = list(jobserver_remote._jobserver_clients)

    async def asyncTearDown(self):
        jobserver_remote._jobserver_clients[:] = self._saved_clients

    async def test_run_transformation_retries_restartable_client_once(self):
        client = _FakeJobserverClient(
            [
                ClientRestartRequiredError("busy"),
                "0" * 64,
            ]
        )
        jobserver_remote._jobserver_clients[:] = [client]
        result = await jobserver_remote.run_transformation(
            {"__language__": "python"},
            tf_checksum="1" * 64,
            tf_dunder={},
            scratch=False,
        )
        self.assertEqual(str(result), "0" * 64)
        self.assertEqual(client.calls, 2)
        self.assertEqual(client.restart_calls, 1)

    async def test_run_transformation_does_not_retry_generic_failure(self):
        client = _FakeJobserverClient([RuntimeError("connection refused")])
        jobserver_remote._jobserver_clients[:] = [client]
        with self.assertRaises(RuntimeError):
            await jobserver_remote.run_transformation(
                {"__language__": "python"},
                tf_checksum="1" * 64,
                tf_dunder={},
                scratch=False,
            )
        self.assertEqual(client.calls, 1)
        self.assertEqual(client.restart_calls, 0)

    async def test_run_transformation_cancels_jobserver_when_await_is_cancelled(self):
        client = _FakeJobserverClient([asyncio.CancelledError()])
        jobserver_remote._jobserver_clients[:] = [client]
        with self.assertRaises(asyncio.CancelledError):
            await jobserver_remote.run_transformation(
                {"__language__": "python"},
                tf_checksum="2" * 64,
                tf_dunder={},
                scratch=False,
            )
        self.assertEqual(client.calls, 1)
        self.assertEqual(client.cancel_calls, ["2" * 64])
