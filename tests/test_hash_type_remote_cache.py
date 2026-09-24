import asyncio

import pytest

from seamless import Buffer, Checksum
from seamless.caching import buffer_writer
from seamless.checksum.hash_type import (
    Kind,
    Length,
    get_hash_type_cache,
    get_hash_type_remote,
    pack,
    register_hash_type_for_buffer_async,
    set_hash_type,
)
from helpers.fake_remotes import install_fake_remotes


def test_hash_type_async_lookup_writes_and_reads_remote(monkeypatch):
    rows = {}
    install_fake_remotes(monkeypatch, {}, {}, [], hash_type_rows=rows)
    buffer = Buffer(b"hello")
    checksum = buffer.get_checksum()
    get_hash_type_cache().clear()

    computed = asyncio.run(register_hash_type_for_buffer_async(checksum, buffer))
    buffer_writer.flush()
    assert rows[checksum.hex()] == computed.word

    get_hash_type_cache().clear()
    loaded = asyncio.run(get_hash_type_remote(checksum))

    assert loaded == computed
    assert get_hash_type_cache()[checksum] == computed.word


def test_hash_type_async_lookup_rejects_invalid_remote_word(monkeypatch):
    buffer = Buffer(b"hello")
    checksum = buffer.get_checksum()
    rows = {checksum.hex(): 8192}
    install_fake_remotes(monkeypatch, {}, {}, [], hash_type_rows=rows)
    get_hash_type_cache().clear()

    with pytest.raises(ValueError):
        asyncio.run(get_hash_type_remote(checksum))


def test_tightening_uploads_new_word(monkeypatch):
    rows = {}
    calls = []
    install_fake_remotes(monkeypatch, {}, {}, calls, hash_type_rows=rows)
    buffer = Buffer(b'"hello"')
    checksum = buffer.get_checksum()
    loose = pack(Kind.JSON_UNTESTED, Length.SHORT)
    tight = pack(Kind.JSON_STRING, Length.SHORT)
    buffer_writer.flush()
    calls.clear()
    get_hash_type_cache().clear()
    get_hash_type_cache()[checksum] = loose

    computed = asyncio.run(register_hash_type_for_buffer_async(checksum, buffer))
    buffer_writer.flush()

    assert computed.word == tight
    assert rows[checksum.hex()] == tight
    assert calls.count("database:set_hash_type") == 1


@pytest.mark.parametrize(
    "incoming",
    [
        pack(Kind.JSON_STRING, Length.SHORT),
        pack(Kind.JSON_UNTESTED, Length.SHORT),
    ],
    ids=["equal", "looser"],
)
def test_equal_or_looser_write_uploads_nothing(monkeypatch, incoming):
    rows = {}
    calls = []
    install_fake_remotes(monkeypatch, {}, {}, calls, hash_type_rows=rows)
    checksum = Checksum("a" * 64)
    tight = pack(Kind.JSON_STRING, Length.SHORT)
    get_hash_type_cache().clear()
    get_hash_type_cache()[checksum] = tight

    set_hash_type(checksum, incoming)
    buffer_writer.flush()

    assert "database:set_hash_type" not in calls
    assert rows == {}


def test_sync_tightening_uploads(monkeypatch):
    rows = {}
    calls = []
    install_fake_remotes(monkeypatch, {}, {}, calls, hash_type_rows=rows)
    checksum = Checksum("b" * 64)
    loose = pack(Kind.JSON_UNTESTED, Length.SHORT)
    tight = pack(Kind.JSON_STRING, Length.SHORT)
    buffer_writer.flush()
    calls.clear()
    get_hash_type_cache().clear()
    get_hash_type_cache()[checksum] = loose

    set_hash_type(checksum, tight)
    buffer_writer.flush()

    assert rows[checksum.hex()] == tight
    assert calls.count("database:set_hash_type") == 1


def test_write_through_does_not_wait_for_database(monkeypatch):
    import threading
    from seamless_remote import database_remote

    started, finish = threading.Event(), threading.Event()
    rows = {}

    async def write(checksum, word):
        started.set()
        await asyncio.to_thread(finish.wait)
        rows[checksum.hex()] = word
        return True

    buffer_writer.flush()
    monkeypatch.setattr(database_remote, "set_hash_type", write)
    checksum = Checksum("9" * 64)
    word = pack(Kind.JSON_STRING, Length.SHORT)
    get_hash_type_cache().pop(checksum, None)
    try:
        set_hash_type(checksum, word)
        assert started.wait(2)
        assert rows == {}
    finally:
        finish.set()
        buffer_writer.flush(timeout=5)
    assert rows == {checksum.hex(): word}
