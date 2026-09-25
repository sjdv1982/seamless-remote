"""Coverage for contracts/expressions.md, Cancellation > The linger, and the
client-side dispatch retry rule (Placement > Rules).

The linger is "a few seconds, not contractual", at most a test hook: these
tests shorten it through the module constant and assert only its observable
consequences.
"""

import asyncio

import pytest

from seamless import Buffer, Checksum, Expression
from seamless.checksum import expression as expression_mod
from seamless.checksum.expression import _active_expressions, get_expression_cache
from helpers.fake_remotes import _install_fake_remotes


_LINGER = 0.3


@pytest.fixture(autouse=True)
def short_linger(monkeypatch):
    get_expression_cache().clear()
    _active_expressions.clear()
    monkeypatch.setattr(expression_mod, "_EXPRESSION_LINGER", _LINGER)
    yield
    get_expression_cache().clear()
    _active_expressions.clear()


def _gated_jobserver(monkeypatch, result_checksum, calls):
    from seamless_remote import jobserver_remote

    started = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()

    async def run_expression(input_checksum, path, celltype, target_celltype, *, scratch):
        calls.append("jobserver:run")
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return result_checksum

    monkeypatch.setattr(jobserver_remote, "run_expression", run_expression)
    return started, release, cancelled


def _softcancel(expression):
    return type(expression).softcancel(expression)


def test_requester_arriving_during_the_linger_rejoins_the_fetch(monkeypatch):
    """expressions.md, Cancellation: a requester during the linger re-registers."""
    source_checksum = Checksum("8" * 64)
    result_checksum = Buffer("rejoined", "str").get_checksum()
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    started, release, cancelled = _gated_jobserver(monkeypatch, result_checksum, calls)
    first = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
    second = Expression(source_checksum, "a", input_celltype="plain", celltype="str")

    async def main():
        task1 = asyncio.create_task(first.compute_async(execution="remote"))
        await asyncio.wait_for(started.wait(), 5)
        assert _softcancel(first) is True
        with pytest.raises(asyncio.CancelledError):
            await task1
        # Inside the linger: the fetch is still alive and the newcomer joins it.
        task2 = asyncio.create_task(second.compute_async(execution="remote"))
        await asyncio.sleep(_LINGER * 3)
        assert not cancelled.is_set(), "rejoining must cancel the pending linger expiry"
        release.set()
        return await asyncio.wait_for(task2, 5)

    assert asyncio.run(main()) == result_checksum
    assert calls.count("jobserver:run") == 1


def test_fetch_completing_during_the_linger_is_recorded_and_usable(monkeypatch):
    """expressions.md, The linger: it may still complete; its result is recorded."""
    source_checksum = Checksum("9" * 64)
    result_checksum = Buffer("late but kept", "str").get_checksum()
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    started, release, cancelled = _gated_jobserver(monkeypatch, result_checksum, calls)
    expression = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
    key = (source_checksum.hex(), "a", "plain", "str")

    async def main():
        task = asyncio.create_task(expression.compute_async(execution="remote"))
        await asyncio.wait_for(started.wait(), 5)
        assert _softcancel(expression) is True
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        await asyncio.sleep(_LINGER * 2)

    asyncio.run(main())
    assert not cancelled.is_set()
    assert get_expression_cache().get(key) == result_checksum
    assert _softcancel(expression) is False


def test_fetch_is_aborted_once_the_linger_expires_with_no_waiter(monkeypatch):
    """expressions.md, Cancellation: softcancel = deregister; abort after a linger."""
    source_checksum = Checksum("a" * 64)
    result_checksum = Buffer("never arrives", "str").get_checksum()
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    started, release, cancelled = _gated_jobserver(monkeypatch, result_checksum, calls)
    expression = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
    key = (source_checksum.hex(), "a", "plain", "str")

    async def main():
        task = asyncio.create_task(expression.compute_async(execution="remote"))
        await asyncio.wait_for(started.wait(), 5)
        assert _softcancel(expression) is True
        assert _softcancel(expression) is False  # idempotent
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(_LINGER / 10)
        assert not cancelled.is_set()
        await asyncio.wait_for(cancelled.wait(), _LINGER * 10)

    asyncio.run(main())
    assert key not in get_expression_cache()
    assert not _active_expressions


def test_restart_required_is_retried_once_then_the_next_client(monkeypatch):
    from seamless_remote import jobserver_remote
    from seamless_remote.client import ClientRestartRequiredError

    result = Checksum("d" * 64)

    class Client:
        def __init__(self, responses):
            self.responses = list(responses)
            self.calls = 0
            self.restarts = 0

        async def run_expression(self, *args, **kwargs):
            self.calls += 1
            response = self.responses.pop(0)
            if isinstance(response, BaseException):
                raise response
            return response

        def restart(self):
            self.restarts += 1

    busy = Client([ClientRestartRequiredError("busy"), ClientRestartRequiredError("busy")])
    healthy = Client([result])
    monkeypatch.setattr(jobserver_remote, "_jobserver_clients", [busy, healthy])

    actual = asyncio.run(
        jobserver_remote.run_expression(Checksum("c" * 64), "a", "plain", "str", scratch=True)
    )
    assert actual == result
    assert busy.calls == 2
    assert healthy.calls == 1
