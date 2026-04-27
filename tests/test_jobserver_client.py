import sys
import unittest
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_seamless = ModuleType("seamless")
_seamless.__path__ = []
_seamless_util = ModuleType("seamless.util")
_seamless_util.__path__ = []
_seamless_pylru = ModuleType("seamless.util.pylru")


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


_seamless.Checksum = _Checksum
_seamless.is_worker = lambda: False
_seamless.ensure_open = lambda *args, **kwargs: None
_seamless_pylru.lrucache = lambda size: {}
_seamless_util.pylru = _seamless_pylru
_seamless.util = _seamless_util
sys.modules["seamless"] = _seamless
sys.modules["seamless.util"] = _seamless_util
sys.modules["seamless.util.pylru"] = _seamless_pylru

_remote_job = ModuleType("seamless_transformer.remote_job")
_remote_job.parse_remote_job_written = lambda value: None
sys.modules["seamless_transformer.remote_job"] = _remote_job

from seamless_remote.jobserver_client import JobserverClient  # noqa: E402


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
    def __init__(self, text):
        self.text = text

    def get(self, path, json=None):
        del path, json
        return _Response(text=self.text)


class JobserverClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_transformation_parses_structured_success_payload(self):
        client = JobserverClient()
        client.url = "http://jobserver.invalid"
        client._initialized = True
        client._get_session = lambda: _FakeSession(
            '{"result_checksum": "%s", "probe_context": {"required_bucket_checksums": {"node": "%s"}}, "compilation_context": "%s", "job_validation": {"job_contract_violations": ["runpath_outside_conda_prefix"], "diagnostics": {"compiled": true}}, "record_runtime": {"started_at": "2026-04-27T10:00:00Z", "finished_at": "2026-04-27T10:00:03Z", "wall_time_seconds": 3.0, "cpu_user_seconds": 1.2, "cpu_system_seconds": 0.4, "memory_peak_bytes": 123456, "hostname": "jobserver-worker-1", "pid": 4321, "process_started_at": "2026-04-27T09:00:00Z", "worker_execution_index": 17, "retry_count": 1}}'
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
                "hostname": "jobserver-worker-1",
                "pid": 4321,
                "process_started_at": "2026-04-27T09:00:00Z",
                "worker_execution_index": 17,
                "retry_count": 1,
            },
        )
