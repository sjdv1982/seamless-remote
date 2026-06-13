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
_seamless_checksum = ModuleType("seamless.checksum")
_seamless_checksum.__path__ = []
_seamless_hash_type = ModuleType("seamless.checksum.hash_type")
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


class _HashType:
    @staticmethod
    def is_valid_word(value):
        return isinstance(value, int) and 0 <= value < 8192


_seamless_hash_type.HashType = _HashType
_seamless_pylru.lrucache = lambda size: {}
_seamless.checksum = _seamless_checksum
_seamless_checksum.hash_type = _seamless_hash_type
_seamless_util.pylru = _seamless_pylru
_seamless.util = _seamless_util
sys.modules["seamless"] = _seamless
sys.modules["seamless.checksum"] = _seamless_checksum
sys.modules["seamless.checksum.hash_type"] = _seamless_hash_type
sys.modules["seamless.util"] = _seamless_util
sys.modules["seamless.util.pylru"] = _seamless_pylru

from aiohttp import web  # noqa: E402

from database import DatabaseServer, format_response  # noqa: E402
from database_models import _db, db_init  # noqa: E402
from seamless_remote.client import close_all_clients  # noqa: E402
from seamless_remote.database_client import DatabaseClient  # noqa: E402


TF_CHECKSUM = "1" * 64
RESULT_CHECKSUM = "2" * 64
BUCKET_CHECKSUM = "3" * 64
EXPR_INPUT_CHECKSUM = "5" * 64
EXPR_RESULT_CHECKSUM = "6" * 64
EXPR_OTHER_RESULT_CHECKSUM = "7" * 64
HASH_TYPE_WORD = 4
HASH_TYPE_OTHER_WORD = 5
HASH_TYPE_INVALID_WORD = 8192


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

    def get(self, path, json=None, **kwargs):
        del kwargs
        if str(path).endswith("/healthcheck"):
            return _StaticRequest(_Response(200, "OK"))
        return _FakeRequest("GET", self.server, json or {})

    def put(self, path, json=None, **kwargs):
        del path, kwargs
        return _FakeRequest("PUT", self.server, json or {})


class _StaticRequest:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeRequest:
    def __init__(self, method: str, server: DatabaseServer, request: dict):
        self.method = method
        self.server = server
        self.request = request

    async def __aenter__(self):
        checksum = self.request.get("checksum")
        if self.request["type"] == "bucket_probe":
            checksum = None
        if self.method == "GET":
            payload = await self.server._get(
                self.request["type"], checksum, self.request
            )
        else:
            payload = await self.server._put(
                self.request["type"], checksum, self.request
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

    async def test_duplicate_execution_record_conflict_is_nonfatal(self):
        record = _record()
        changed_record = {**record, "checksum_fields": ["environment"]}

        result = await self.client.set_execution_record(
            TF_CHECKSUM, RESULT_CHECKSUM, record
        )
        self.assertIsNone(result)
        result = await self.client.set_execution_record(
            TF_CHECKSUM, RESULT_CHECKSUM, changed_record
        )
        self.assertIs(result, False)
        self.assertEqual(await self.client.get_execution_record(TF_CHECKSUM), record)

    async def test_bucket_probe_roundtrip_and_overwrite(self):
        probe = {
            "bucket_kind": "environment",
            "label": "conda:/envs/seamless1",
            "bucket_checksum": BUCKET_CHECKSUM,
            "captured_at": "2026-04-26T12:00:00Z",
            "freshness_tokens": {"conda_meta_mtime": 123},
        }
        await self.client.set_bucket_probe(
            probe["bucket_kind"],
            probe["label"],
            probe["bucket_checksum"],
            probe["freshness_tokens"],
            probe["captured_at"],
        )
        self.assertEqual(
            await self.client.get_bucket_probe(
                probe["bucket_kind"], probe["label"]
            ),
            probe,
        )

        updated_probe = {
            **probe,
            "bucket_checksum": "4" * 64,
            "captured_at": "2026-04-26T12:05:00Z",
            "freshness_tokens": {"conda_meta_mtime": 456},
        }
        await self.client.set_bucket_probe(
            updated_probe["bucket_kind"],
            updated_probe["label"],
            updated_probe["bucket_checksum"],
            updated_probe["freshness_tokens"],
            updated_probe["captured_at"],
        )
        self.assertEqual(
            await self.client.get_bucket_probe(
                updated_probe["bucket_kind"], updated_probe["label"]
            ),
            updated_probe,
        )

    async def test_expression_result_roundtrip_and_reverse_lookup(self):
        result = await self.client.get_expression_result(
            EXPR_INPUT_CHECKSUM, "a", "plain", "mixed"
        )
        self.assertIsNone(result)

        await self.client.set_expression_result(
            EXPR_INPUT_CHECKSUM,
            "a",
            "plain",
            "mixed",
            EXPR_RESULT_CHECKSUM,
        )

        result = await self.client.get_expression_result(
            EXPR_INPUT_CHECKSUM, "a", "plain", "mixed"
        )
        self.assertEqual(result.hex(), EXPR_RESULT_CHECKSUM)

        rev = await self.client.get_rev_expressions(EXPR_RESULT_CHECKSUM)
        self.assertEqual(len(rev), 1)
        self.assertEqual(rev[0]["checksum"].hex(), EXPR_INPUT_CHECKSUM)
        self.assertEqual(rev[0]["path"], "a")
        self.assertEqual(rev[0]["celltype"], "plain")
        self.assertEqual(rev[0]["target_celltype"], "mixed")
        self.assertEqual(rev[0]["result"].hex(), EXPR_RESULT_CHECKSUM)

    async def test_expression_result_conflict_is_nonfatal(self):
        result = await self.client.set_expression_result(
            EXPR_INPUT_CHECKSUM,
            "[0]",
            "bytes",
            "int",
            EXPR_RESULT_CHECKSUM,
        )
        self.assertIsNone(result)
        result = await self.client.set_expression_result(
            EXPR_INPUT_CHECKSUM,
            "[0]",
            "bytes",
            "int",
            EXPR_OTHER_RESULT_CHECKSUM,
        )
        self.assertIs(result, False)

        result = await self.client.get_expression_result(
            EXPR_INPUT_CHECKSUM, "[0]", "bytes", "int"
        )
        self.assertEqual(result.hex(), EXPR_RESULT_CHECKSUM)

    async def test_hash_type_roundtrip(self):
        result = await self.client.get_hash_type(EXPR_INPUT_CHECKSUM)
        self.assertIsNone(result)

        result = await self.client.set_hash_type(EXPR_INPUT_CHECKSUM, HASH_TYPE_WORD)
        self.assertIsNone(result)

        result = await self.client.get_hash_type(EXPR_INPUT_CHECKSUM)
        self.assertEqual(result, HASH_TYPE_WORD)

    async def test_hash_type_conflict_is_nonfatal(self):
        result = await self.client.set_hash_type(EXPR_INPUT_CHECKSUM, HASH_TYPE_WORD)
        self.assertIsNone(result)
        result = await self.client.set_hash_type(
            EXPR_INPUT_CHECKSUM, HASH_TYPE_OTHER_WORD
        )
        self.assertIs(result, False)

        result = await self.client.get_hash_type(EXPR_INPUT_CHECKSUM)
        self.assertEqual(result, HASH_TYPE_WORD)

    async def test_hash_type_rejects_invalid_words_client_side(self):
        with self.assertRaises(ValueError):
            await self.client.set_hash_type(
                EXPR_INPUT_CHECKSUM, HASH_TYPE_INVALID_WORD
            )
