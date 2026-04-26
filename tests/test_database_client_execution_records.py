import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATABASE_DIR = ROOT / "seamless-database"
if str(DATABASE_DIR) not in sys.path:
    sys.path.insert(0, str(DATABASE_DIR))

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

from aiohttp import web  # noqa: E402

from database import DatabaseServer, format_response  # noqa: E402
from database_models import _db, db_init  # noqa: E402
from seamless_remote.client import close_all_clients  # noqa: E402
from seamless_remote.database_client import DatabaseClient  # noqa: E402


TF_CHECKSUM = "1" * 64
RESULT_CHECKSUM = "2" * 64


def _record():
    return {
        "schema_version": 1,
        "checksum_fields": ["node"],
        "tf_checksum": TF_CHECKSUM,
        "result_checksum": RESULT_CHECKSUM,
    }


class _Response:
    def __init__(self, status: int, text: str):
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self, server: DatabaseServer):
        self.server = server

    def get(self, path, json=None):
        del path
        return _FakeRequest("GET", self.server, json or {})

    def put(self, path, json=None):
        del path
        return _FakeRequest("PUT", self.server, json or {})


class _FakeRequest:
    def __init__(self, method: str, server: DatabaseServer, request: dict):
        self.method = method
        self.server = server
        self.request = request

    async def __aenter__(self):
        if self.method == "GET":
            payload = await self.server._get(
                self.request["type"], self.request["checksum"], self.request
            )
        else:
            payload = await self.server._put(
                self.request["type"], self.request["checksum"], self.request
            )
        if isinstance(payload, web.Response):
            text = payload.text
            if text is None and payload.body is not None:
                text = payload.body.decode()
            self.response = _Response(payload.status, text or "")
            return self.response
        status, body = format_response(payload, none_as_404=True)
        if status is None:
            status = 200
        if isinstance(body, bytes):
            body = body.decode()
        self.response = _Response(status, body)
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DatabaseClientExecutionRecordTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.dbfile = Path(self._tmpdir.name) / "seamless.db"
        if not _db.is_closed():
            _db.close()
        db_init(str(self.dbfile))
        self.server = DatabaseServer("127.0.0.1", 0, writable=True)
        self.client = DatabaseClient(readonly=False)
        self.client.url = "http://database.invalid"
        self.client._initialized = True
        self.client._get_session = lambda: _FakeSession(self.server)

    async def asyncTearDown(self):
        close_all_clients()
        if not _db.is_closed():
            _db.close()
        self._tmpdir.cleanup()

    async def test_execution_record_roundtrip_and_irreproducible_lookup(self):
        record = _record()
        await self.client.set_execution_record(TF_CHECKSUM, RESULT_CHECKSUM, record)
        self.assertEqual(
            await self.client.get_execution_record(TF_CHECKSUM),
            record,
        )
        self.assertIsNone(
            await self.client.get_irreproducible_records(TF_CHECKSUM)
        )

        await self.client.undo_transformation_result(TF_CHECKSUM, RESULT_CHECKSUM)

        records = await self.client.get_irreproducible_records(TF_CHECKSUM)
        self.assertEqual(
            records,
            [
                {
                    "checksum": TF_CHECKSUM,
                    "result": RESULT_CHECKSUM,
                    "metadata": record,
                }
            ],
        )
        self.assertEqual(
            await self.client.get_irreproducible_records(
                TF_CHECKSUM, RESULT_CHECKSUM
            ),
            records,
        )
