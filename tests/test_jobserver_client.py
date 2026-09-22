import sys
import unittest
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import ModuleType

import pytest
from aiohttp import ClientConnectionError


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_seamless = ModuleType("seamless")
_seamless.__path__ = []
_seamless_util = ModuleType("seamless.util")
_seamless_util.__path__ = []
_seamless_pylru = ModuleType("seamless.util.pylru")
_seamless_error_envelope = ModuleType("seamless.error_envelope")


class _Checksum:
    def __init__(self, value):
        if isinstance(value, _Checksum):
            value = value._hex
        self._hex = str(value)

    def hex(self):
        return self._hex

    def __str__(self):
        return self._hex

    def __repr__(self):
        return f"Checksum({self._hex!r})"


class _CacheMissError(Exception):
    def __init__(self, checksum):
        self.checksum = _Checksum(checksum)
        super().__init__(self.checksum)


class _WorkflowExecutionError(Exception):
    def __init__(self, message, *, kind="execution"):
        self.kind = kind
        super().__init__(message)


def _envelope_to_error(body):
    error = body.get("error", body)
    kind = error["kind"]
    message = error.get("message", "")
    if kind == "cache_miss":
        return _CacheMissError(error["checksum"])
    return _WorkflowExecutionError(message, kind=kind)


_seamless.Checksum = _Checksum
_seamless.CacheMissError = _CacheMissError
_seamless.WorkflowExecutionError = _WorkflowExecutionError
_seamless.is_worker = lambda: False
_seamless.ensure_open = lambda *args, **kwargs: None
_seamless_error_envelope.envelope_to_error = _envelope_to_error
_seamless_pylru.lrucache = lambda size: {}
_seamless_util.pylru = _seamless_pylru
_seamless.util = _seamless_util
sys.modules["seamless"] = _seamless
sys.modules["seamless.util"] = _seamless_util
sys.modules["seamless.util.pylru"] = _seamless_pylru
sys.modules["seamless.error_envelope"] = _seamless_error_envelope

_remote_job = ModuleType("seamless_transformer.remote_job")
_remote_job.parse_remote_job_written = lambda value: None
sys.modules["seamless_transformer.remote_job"] = _remote_job
_record_runtime = ModuleType("seamless_transformer.record_runtime")
_record_runtime.get_record_mode = lambda: False
sys.modules["seamless_transformer.record_runtime"] = _record_runtime

from seamless_remote.jobserver_client import JobserverClient  # noqa: E402
import seamless_remote.jobserver_client as jobserver_client  # noqa: E402


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


class _FakeSession:
    def __init__(self, text="", *, status=200, exception=None):
        self.text = text
        self.status = status
        self.exception = exception
        self.requests = []

    def get(self, path, json=None):
        self.requests.append((path, json))
        if self.exception is not None:
            raise self.exception
        return _Response(status=self.status, text=self.text)


class JobserverClientTests(unittest.IsolatedAsyncioTestCase):
    async def _run_expression_without_retries(self, session):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        client._get_session = lambda: session
        return await JobserverClient.run_expression.__wrapped__(
            client, "1" * 64, "a", "plain", "str"
        )

    async def test_run_expression_parses_structured_success_payload(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        session = _FakeSession('{"result_checksum": "%s"}' % ("9" * 64))
        client._get_session = lambda: session

        result = await client.run_expression("1" * 64, "a", "plain", "str")

        self.assertEqual(str(result), "9" * 64)
        self.assertEqual(
            session.requests,
            [
                (
                    "http://jobserver.invalid/run-expression",
                    {
                        "input_checksum": "1" * 64,
                        "path": "a",
                        'input_celltype': "plain",
                        'celltype': "str",
                    },
                )
            ],
        )

    async def test_run_expression_carries_the_requesters_scratch_decision(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        session = _FakeSession('{"result_checksum": "%s"}' % ("9" * 64))
        client._get_session = lambda: session

        result = await client.run_expression(
            "1" * 64, "a", "plain", "str", scratch=True
        )

        self.assertEqual(str(result), "9" * 64)
        self.assertTrue(session.requests[0][1]["scratch"])

    async def test_run_transformation_parses_structured_success_payload(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        client._get_session = lambda: _FakeSession(
            '{"result_checksum": "%s", "probe_context": {"required_bucket_checksums": {"node": "%s"}}, "compilation_context": "%s", "job_validation": {"job_contract_violations": ["runpath_outside_conda_prefix"], "diagnostics": {"compiled": true}}, "record_runtime": {"started_at": "2026-04-27T10:00:00Z", "finished_at": "2026-04-27T10:00:03Z", "wall_time_seconds": 3.0, "cpu_user_seconds": 1.2, "cpu_system_seconds": 0.4, "memory_peak_bytes": 123456, "gpu_memory_peak_bytes": 444555666, "compilation_time_seconds": 1.75, "hostname": "jobserver-worker-1", "pid": 4321, "process_started_at": "2026-04-27T09:00:00Z", "process_create_time_epoch": 9876.5, "worker_execution_index": 17, "retry_count": 1}}'
            % ("2" * 64, "3" * 64, "4" * 64)
        )

        result = await client.run_transformation(
            {"__language__": "python"},
            tf_checksum="1" * 64,
            tf_dunder={},
            scratch=False,
        )

        self.assertEqual(str(result["result_checksum"]), "2" * 64)
        self.assertEqual(
            result["probe_context"],
            {"required_bucket_checksums": {"node": "3" * 64}},
        )
        self.assertEqual(result["compilation_context"], "4" * 64)
        self.assertEqual(
            result["job_validation"],
            {
                "job_contract_violations": ["runpath_outside_conda_prefix"],
                "diagnostics": {"compiled": True},
            },
        )
        self.assertEqual(
            result["record_runtime"],
            {
                "started_at": "2026-04-27T10:00:00Z",
                "finished_at": "2026-04-27T10:00:03Z",
                "wall_time_seconds": 3.0,
                "cpu_user_seconds": 1.2,
                "cpu_system_seconds": 0.4,
                "memory_peak_bytes": 123456,
                "gpu_memory_peak_bytes": 444555666,
                "compilation_time_seconds": 1.75,
                "hostname": "jobserver-worker-1",
                "pid": 4321,
                "process_started_at": "2026-04-27T09:00:00Z",
                "process_create_time_epoch": 9876.5,
                "worker_execution_index": 17,
                "retry_count": 1,
            },
        )

    async def test_run_transformation_sends_record_mode(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        session = _FakeSession('{"result_checksum": "%s"}' % ("2" * 64))
        client._get_session = lambda: session

        old_get_record_mode = jobserver_client.get_record_mode
        try:
            jobserver_client.get_record_mode = lambda: True
            await client.run_transformation(
                {"__language__": "python"},
                tf_checksum="1" * 64,
                tf_dunder={},
                scratch=False,
            )
        finally:
            jobserver_client.get_record_mode = old_get_record_mode

        self.assertEqual(session.requests[0][1]["record"], True)

    async def test_run_transformation_parses_structured_remote_job_payload(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        client._get_session = lambda: _FakeSession(
            '{"remote_job_written": "REMOTE_JOB_WRITTEN:/tmp/jobdir"}'
        )

        result = await client.run_transformation(
            {"__language__": "bash"},
            tf_checksum="1" * 64,
            tf_dunder={},
            scratch=False,
        )

        self.assertEqual(
            result,
            {"remote_job_written": "REMOTE_JOB_WRITTEN:/tmp/jobdir", "record_runtime": None},
        )

    async def test_cache_miss_response_raises_cache_miss_error(self):
        checksum = "2" * 64
        session = _FakeSession(
            '{"error": {"kind": "cache_miss", "message": "missing", '
            f'"checksum": "{checksum}"}}}}'
        )

        with self.assertRaises(_CacheMissError) as info:
            await self._run_expression_without_retries(session)

        self.assertEqual(info.exception.checksum.hex(), checksum)
        self.assertEqual(str(info.exception), checksum)

    async def test_transport_and_malformed_bodies_stay_connection_errors(self):
        sessions = [
            _FakeSession(exception=ClientConnectionError("network down")),
            _FakeSession("server crash", status=500),
            _FakeSession("not json"),
            _FakeSession('{"error": {"message": "kind missing"}}'),
        ]

        for session in sessions:
            with self.subTest(session=session):
                with self.assertRaises(ClientConnectionError):
                    await self._run_expression_without_retries(session)

    async def test_unknown_kind_is_an_execution_error(self):
        session = _FakeSession(
            '{"error": {"kind": "new_failure", "message": "new server failure"}}'
        )

        with self.assertRaises(_WorkflowExecutionError) as info:
            await self._run_expression_without_retries(session)

        self.assertEqual(str(info.exception), "new server failure")
        self.assertEqual(info.exception.kind, "new_failure")
