"""Expression cancellation and shared-fetch contracts (expressions.md,
Cancellation; Buffer fetches are shared by checksum).

Cancellation is keyed by the Expression identity: equal Expressions share one
evaluation, softcancel() deregisters one member, and the last member leaving
starts a short linger before the shared evaluation is cancelled. Distinct
Expressions over the same input are independent members of independent
evaluations.

Below that, the buffer layer shares concurrent fetches of one checksum: one
in-flight fetch, anonymous participants, aborted as soon as the last one
leaves (no linger at that layer), and a failed fetch is not remembered.

Each scenario has a fresh interpreter so shutdown really audits its own
reference accounting. No external buffer service is needed.
"""

import subprocess
import sys
import textwrap

import pytest


_SETUP = """
import asyncio
import logging
import seamless
from seamless import Buffer, Checksum, Expression
from seamless.caching.buffer_cache import get_buffer_cache
from seamless.checksum.cached_calculate_checksum import checksum_cache
from seamless_remote import buffer_remote

logging.basicConfig(level=logging.WARNING)
source = Buffer({'left': 'left contract value', 'right': 'right contract value'}, 'plain')
checksum = source.get_checksum()
cache = get_buffer_cache()
with cache.lock:
    cache.weak_cache.pop(checksum, None)
    cache.strong_cache.pop(checksum, None)
checksum_cache.pop(checksum, None)

class SlowSource:
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.aborted = asyncio.Event()
        self.completed = asyncio.Event()
        self.calls = 0
        self.ignore_cancel = False
        self.misses = 0  # answer this many fetches with "not found"

    async def get(self, requested):
        assert requested == checksum
        self.calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.aborted.set()
            if not self.ignore_cancel:
                raise
            await self.release.wait()
        if self.misses:
            self.misses -= 1
            return None
        # Exercise actual buffer publication/accounting when a late fetch wins.
        source.tempref()
        self.completed.set()
        return source

async def finish(source_client, tasks):
    source_client.release.set()
    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)

def expression(path):
    return Expression(checksum, path, input_celltype='plain', celltype='str')

def softcancel(expr):
    return expr.softcancel()

async def setup():
    client = SlowSource()
    buffer_remote._read_folders_clients = []
    buffer_remote._read_server_clients = [client]
    return client

def observe_fetch_requests(count):
    # Set the returned event once `count` requests for `checksum` have entered
    # the buffer layer's fetch entry point; by then each has joined a fetch.
    entered = asyncio.Event()
    original = buffer_remote.get_buffer
    requests = 0
    async def observe(requested):
        nonlocal requests
        if requested == checksum:
            requests += 1
            if requests == count:
                entered.set()
        return await original(requested)
    buffer_remote.get_buffer = observe
    return entered

def resolve():
    return asyncio.create_task(checksum.resolution())
"""


def _run(body):
    script = _SETUP + '\n' + textwrap.dedent(body)
    result = subprocess.run(
        [sys.executable, '-c', script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'CONTRACT_OK' in result.stdout
    return result


def test_only_waiter_softcancel_aborts_materialization_after_linger():
    _run('''
        async def main():
            client = await setup()
            expr = expression('left')
            task = asyncio.create_task(expr.compute_async(execution='local'))
            try:
                await asyncio.wait_for(client.started.wait(), 5)
                assert softcancel(expr) is True
                # A linger must keep the fetch alive beyond immediate task
                # cancellation; Appendix B specifies a few seconds, not an API
                # constant. The generous upper bound also catches no abort.
                await asyncio.sleep(0.05)
                assert not client.aborted.is_set(), 'fetch aborted without linger'
                await asyncio.wait_for(client.aborted.wait(), 10)
                assert not client.completed.is_set()
            finally:
                await finish(client, [task])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_softcancel_of_one_expression_leaves_a_distinct_expression_running():
    # expressions.md, Cancellation: different Expressions do not share a
    # cancellation set merely because they need the same input buffer. Their
    # shared fetch is asserted separately, in the shared-fetch tests below.
    _run('''
        async def main():
            client = await setup()
            left, right = expression('left'), expression('right')
            assert left != right  # different paths, same input checksum
            first = asyncio.create_task(left.compute_async(execution='local'))
            second = asyncio.create_task(right.compute_async(execution='local'))
            try:
                await asyncio.wait_for(client.started.wait(), 5)
                await asyncio.sleep(0.05)
                assert softcancel(left) is True
                # Outlast left's linger (3 s internal constant, not contract).
                await asyncio.sleep(4.0)
                assert not second.done()
                client.release.set()
                result = await asyncio.wait_for(second, 5)
                assert result == Buffer('right contract value', 'str').get_checksum()
            finally:
                await finish(client, [first, second])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_close_audit_clean_when_fetch_finishes_after_last_waiter_leaves():
    result = _run('''
        async def main():
            client = await setup()
            client.ignore_cancel = True  # model a source already finishing I/O
            expr = expression('left')
            task = asyncio.create_task(expr.compute_async(execution='local'))
            try:
                await asyncio.wait_for(client.started.wait(), 5)
                # Task abandonment must unwind the waiter even if the source
                # itself cannot stop; do not wait for the fetch to finish first.
                task.cancel()
                for _ in range(100):
                    if task.done():
                        break
                    await asyncio.sleep(0.01)
                assert task.done(), 'abandoned waiter still owns the fetch'
                assert not client.completed.is_set()
                client.release.set()
                await asyncio.wait_for(client.completed.wait(), 5)
            finally:
                await finish(client, [task])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')
    assert 'seamless.references' not in result.stderr, result.stderr


# ---------------------------------------------------- shared fetch (by checksum)


def test_concurrent_resolutions_of_one_checksum_share_one_fetch():
    _run('''
        async def main():
            client = await setup()
            entered = observe_fetch_requests(3)
            tasks = [resolve() for _ in range(3)]
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                await asyncio.sleep(0.05)
                assert client.calls == 1, f'{client.calls} fetches of one checksum'
                client.release.set()
                results = await asyncio.wait_for(asyncio.gather(*tasks), 5)
                assert all(r.get_checksum() == checksum for r in results)
                assert client.calls == 1
            finally:
                await finish(client, tasks)
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_distinct_expressions_over_one_input_share_one_fetch():
    # Two member sets (different identities), one shared fetch (same input).
    _run('''
        async def main():
            client = await setup()
            entered = observe_fetch_requests(2)
            left, right = expression('left'), expression('right')
            first = asyncio.create_task(left.compute_async(execution='local'))
            second = asyncio.create_task(right.compute_async(execution='local'))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                await asyncio.sleep(0.05)
                assert client.calls == 1, 'same checksum fetched twice'
                client.release.set()
                results = await asyncio.wait_for(asyncio.gather(first, second), 5)
                assert results == [
                    Buffer('left contract value', 'str').get_checksum(),
                    Buffer('right contract value', 'str').get_checksum(),
                ]
                assert client.calls == 1
            finally:
                await finish(client, [first, second])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_shared_fetch_continues_while_a_participant_remains():
    _run('''
        async def main():
            client = await setup()
            entered = observe_fetch_requests(2)
            leaving, staying = resolve(), resolve()
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                leaving.cancel()
                await asyncio.gather(leaving, return_exceptions=True)
                await asyncio.sleep(0.1)
                assert not client.aborted.is_set(), 'aborted with a participant left'
                assert not staying.done()
                client.release.set()
                result = await asyncio.wait_for(staying, 5)
                assert result.get_checksum() == checksum
                assert client.calls == 1
            finally:
                await finish(client, [leaving, staying])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_softcancelled_expression_leaves_the_shared_fetch_to_its_peer():
    # When left's linger expires, its shared evaluation is cancelled and
    # leaves the fetch; right is still a participant, so the fetch goes on.
    _run('''
        async def main():
            client = await setup()
            entered = observe_fetch_requests(2)
            left, right = expression('left'), expression('right')
            first = asyncio.create_task(left.compute_async(execution='local'))
            second = asyncio.create_task(right.compute_async(execution='local'))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                assert softcancel(left) is True
                # Outlast left's linger (3 s internal constant, not contract).
                await asyncio.sleep(4.0)
                assert not client.aborted.is_set(), 'peer lost the shared fetch'
                assert not second.done()
                client.release.set()
                result = await asyncio.wait_for(second, 5)
                assert result == Buffer('right contract value', 'str').get_checksum()
                assert client.calls == 1
            finally:
                await finish(client, [first, second])
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_shared_fetch_is_aborted_as_soon_as_its_last_participant_leaves():
    _run('''
        async def main():
            client = await setup()
            entered = observe_fetch_requests(2)
            tasks = [resolve(), resolve()]
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                # No linger at this layer: the abort follows at once.
                await asyncio.wait_for(client.aborted.wait(), 0.5)
                assert not client.completed.is_set()
                # The aborted fetch is not remembered: a new request fetches.
                client.release.set()
                result = await asyncio.wait_for(resolve(), 5)
                assert result.get_checksum() == checksum
                assert client.calls == 2
            finally:
                await finish(client, tasks)
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')


def test_a_failed_shared_fetch_is_not_remembered():
    _run('''
        from seamless import CacheMissError

        async def main():
            client = await setup()
            client.misses = 1
            entered = observe_fetch_requests(2)
            tasks = [resolve(), resolve()]
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.wait_for(client.started.wait(), 5)
                client.release.set()
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 5
                )
                assert all(isinstance(r, CacheMissError) for r in results), results
                assert client.calls == 1  # both participants shared the one miss
                result = await asyncio.wait_for(resolve(), 5)
                assert result.get_checksum() == checksum
                assert client.calls == 2
            finally:
                await finish(client, tasks)
        asyncio.run(main())
        seamless.close()
        print('CONTRACT_OK')
    ''')
