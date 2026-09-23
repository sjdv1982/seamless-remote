"""Desired Appendix B.6 contracts (known-issues §3.4 item 4).

The checksum waiting set and its few-second linger are settled, but not yet
implemented. Each scenario has a fresh interpreter so shutdown really
audits its own reference accounting. No external buffer service is needed.
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
    method = getattr(type(expr), 'softcancel', None)
    if method is not None:
        return method(expr)
    return expr.cancel()

async def setup():
    client = SlowSource()
    buffer_remote._read_folders_clients = []
    buffer_remote._read_server_clients = [client]
    return client
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


def test_two_distinct_expressions_share_checksum_fetch_after_one_softcancel():
    _run('''
        async def main():
            client = await setup()
            left, right = expression('left'), expression('right')
            assert left != right  # different paths, same input checksum
            entered = asyncio.Event()
            original = Checksum.resolution
            requests = 0
            async def observe(self, *args, **kwargs):
                nonlocal requests
                if self == checksum:
                    requests += 1
                    if requests == 2:
                        entered.set()
                return await original(self, *args, **kwargs)
            Checksum.resolution = observe
            first = asyncio.create_task(left.compute_async(execution='local'))
            second = asyncio.create_task(right.compute_async(execution='local'))
            try:
                await asyncio.wait_for(client.started.wait(), 5)
                await asyncio.wait_for(entered.wait(), 5)
                await asyncio.sleep(0)
                assert client.calls == 1, 'same checksum fetched twice'
                assert softcancel(left) is True
                # Outlast the sole-waiter abort bound in the companion test.
                await asyncio.sleep(10.1)
                assert not client.aborted.is_set()
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


@pytest.mark.xfail(
    strict=False,
    reason="contract ahead of code: materialization linger claims are not implemented",
)
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
