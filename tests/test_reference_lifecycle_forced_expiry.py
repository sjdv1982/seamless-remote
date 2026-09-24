from __future__ import annotations

import gc
from uuid import uuid4

import pytest

from seamless import Buffer, CacheMissError, Cell, Expression
from seamless.caching.buffer_cache import get_buffer_cache
from seamless.reference_lifecycle import collect_refholder_claims
from helpers.reference_lifecycle import force_expiry


def _unique_bytes(label: str) -> bytes:
    return f"{label}-{uuid4().hex}".encode()


def _guard_remote_resolution(monkeypatch):
    calls = {"buffer_remote": 0}
    try:
        import seamless_remote.buffer_remote as buffer_remote
    except ImportError:
        return calls

    async def no_buffer(checksum):
        calls["buffer_remote"] += 1
        return None

    monkeypatch.setattr(buffer_remote, "get_buffer", no_buffer)
    return calls


def _assert_unowned_resolution_fails(monkeypatch, checksum, resolve):
    calls = _guard_remote_resolution(monkeypatch)
    with pytest.raises((CacheMissError, RuntimeError, ValueError)):
        resolve()
    return calls


def _assert_claims(holder, checksum, role):
    claims = collect_refholder_claims([holder]).get(checksum, [])
    assert len(claims) == 1
    assert claims[0][0] is holder
    assert claims[0][1] == role


def test_cell_input_survives_forced_expiry_and_unowned_control(monkeypatch):
    cache = get_buffer_cache()
    source = Buffer(_unique_bytes("cell"), "bytes")
    checksum = source.get_checksum()
    cell = Cell(checksum=checksum, celltype="bytes")
    _assert_claims(cell, checksum, "input")
    del source
    gc.collect()

    evidence = force_expiry(checksum, cache=cache)
    assert evidence.after["accounting"] == (1, 0, True)
    assert cell._input_ref.resolve("bytes").content.startswith(b"cell-")
    assert evidence.after["strong"] is True

    cell._release_refholds()
    assert cache.reference_snapshot().get(checksum, (0, 0, False))[0] == 0
    force_expiry(checksum, cache=cache)
    calls = _assert_unowned_resolution_fails(
        monkeypatch, checksum, lambda: checksum.resolve("bytes")
    )
    assert calls["buffer_remote"] in (0, 1)


def _expression_fixture(path: str = "value"):
    source = Buffer({"value": _unique_bytes("expression").decode()}, "plain")
    input_checksum = source.get_checksum()
    expression = Expression(input_checksum, path, input_celltype="plain", celltype="text")
    return source, expression


def _assert_expression_result_survives(monkeypatch, source, expression):
    result = expression.result
    assert result is not None
    cache = get_buffer_cache()
    evidence = force_expiry(result, cache=cache)
    assert evidence.after["accounting"] == (1, 0, True)
    assert result.resolve("text").startswith("expression-")
    assert evidence.after["strong"] is True
    expression._release_refholds()
    del source
    gc.collect()
    force_expiry(result, cache=cache)
    _assert_unowned_resolution_fails(
        monkeypatch, result, lambda: result.resolve("text")
    )


def test_expression_result_published_before_public_interest_is_retained(monkeypatch):
    source, expression = _expression_fixture()
    result = expression._evaluate_internal()
    assert result is not None
    assert get_buffer_cache().reference_snapshot().get(result, (0, 0, False))[0] == 0
    expression._enable_result_holding()
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    _assert_claims(expression, result, "result")
    _assert_expression_result_survives(monkeypatch, source, expression)


def test_expression_result_interest_before_publication_is_retained(monkeypatch):
    source, expression = _expression_fixture()
    expression._enable_result_holding()
    result = expression._evaluate_internal()
    assert result is not None
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    _assert_expression_result_survives(monkeypatch, source, expression)


def test_repeated_public_expression_access_has_one_result_role(monkeypatch):
    source, expression = _expression_fixture()
    result = expression.compute()
    assert expression.compute() == result
    assert expression.result == result
    assert get_buffer_cache().reference_snapshot()[result][0] == 1
    assert list(collect_refholder_claims([expression])[result])
    _assert_expression_result_survives(monkeypatch, source, expression)


def test_deepcell_top_level_checksum_is_the_only_claim(monkeypatch):
    source = Buffer({"token": uuid4().hex}, "deepcell")
    checksum = source.get_checksum()
    cell = Cell(checksum=checksum, celltype="deepcell")
    claims = collect_refholder_claims([cell]).get(checksum, [])
    assert [(holder, role) for holder, role in claims] == [(cell, "input")]
    evidence = force_expiry(checksum)
    assert evidence.after["accounting"] == (1, 0, True)
    assert checksum.resolve("deepcell")["token"]
    cell._release_refholds()
    del source
    gc.collect()
    force_expiry(checksum)
    _assert_unowned_resolution_fails(monkeypatch, checksum, lambda: checksum.resolve())


def test_equal_expressions_hold_same_result_independently(monkeypatch):
    source, first = _expression_fixture()
    second = Expression(first.input_checksum, first.path, input_celltype=first.input_celltype, celltype=first.celltype)
    result = first._evaluate_internal()
    assert second._publish_result(result) == result
    first._enable_result_holding()
    second._enable_result_holding()
    cache = get_buffer_cache()
    assert cache.reference_snapshot()[result][0] == 2
    claims = collect_refholder_claims([first, second])[result]
    assert [(holder, role) for holder, role in claims] == [
        (first, "result"),
        (second, "result"),
    ]
    force_expiry(result, cache=cache)
    assert result.resolve("text").startswith("expression-")
    first._release_refholds()
    second._release_refholds()
    del source
    gc.collect()
    force_expiry(result, cache=cache)
    _assert_unowned_resolution_fails(monkeypatch, result, lambda: result.resolve("text"))
