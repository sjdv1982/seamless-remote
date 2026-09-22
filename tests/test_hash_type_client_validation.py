import asyncio

import pytest
from aiohttp import ClientConnectionError

from seamless_remote.database_client import DatabaseClient

CHECKSUM = "1" * 64


class _Response:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return "8192"  # Outside the 13-bit HashType word.


class _Session:
    def __init__(self):
        self.requests = []

    def get(self, url, *, json):
        self.requests.append((url, json))
        return _Response()


def test_get_hash_type_rejects_an_invalid_database_word():
    client = DatabaseClient(readonly=True)
    client.url = "http://database.invalid"
    session = _Session()
    client._get_session = lambda: session

    with pytest.raises(ClientConnectionError, match="Malformed response for hash_type"):
        asyncio.run(client._get_hash_type_unthrottled(CHECKSUM))

    assert session.requests == [
        (
            "http://database.invalid",
            {"type": "hash_type", "checksum": CHECKSUM},
        )
    ]
