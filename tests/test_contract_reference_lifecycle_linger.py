"""Contract tests: contracts/internal/checksum-reference-lifecycle.md §1, §6
(*Waiting-set entry* row) and §10 — the waiting set holds one neutral claim on a
shared Expression evaluation's input, keyed by Expression identity. It is
acquired when the shared evaluation starts, held through the linger after the
last member leaves, and released when the task finishes or the linger expires
and the task is cancelled; the shutdown audit stays quiet.

The claim is counted, not looked up by role name: an Expression's own input is
tempref-only (§6) and nothing else holds the input here, so the input has
exactly one refholder reference, the waiting set's. (The code logs it under the
role ``expression materialization``, as §6 says.) The neutrality of that claim
is a §10 gap, pinned by an xfail below. The audit is checked through its logger,
``seamless.references`` (§9), not through message wording.

Fresh interpreter per test, so the shutdown audit really audits this scenario
only. No external buffer service is needed: the read-server client is a stub.
"""

import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_WAITER_TESTS = Path(__file__).with_name("test_materialization_waiter_contract.py")
_SETUP = re.search(r'_SETUP = """(.*?)"""', _WAITER_TESTS.read_text(), re.S).group(1)

_LINGER_BODY = """
def count():
    return cache.reference_snapshot().get(checksum, (0, 0, False))[0]

async def main():
    client = await setup()
    expr = expression('left')
    task = asyncio.create_task(expr.compute_async(execution='local'))
    try:
        await asyncio.wait_for(client.started.wait(), 5)
        assert softcancel(expr) is True
        await asyncio.sleep(0.05)
        assert not client.aborted.is_set()
        assert count() == 1, f'expected one linger claim, found {count()}'
        await asyncio.wait_for(client.aborted.wait(), 10)
        for _ in range(100):
            if count() == 0:
                break
            await asyncio.sleep(0.01)
        assert count() == 0, 'claim survived the abort'
    finally:
        await finish(client, [task])

asyncio.run(main())
seamless.close()
print('CONTRACT_OK')
"""

_IDENTITY_BODY = """
def count():
    return cache.reference_snapshot().get(checksum, (0, 0, False))[0]

async def main():
    client = await setup()
    first, second = expression('left'), expression('left')  # one identity
    tasks = [
        asyncio.create_task(first.compute_async(execution='local')),
        asyncio.create_task(second.compute_async(execution='local')),
    ]
    try:
        await asyncio.wait_for(client.started.wait(), 5)
        await asyncio.sleep(0.05)
        assert client.calls == 1, 'equal Expressions did not share one evaluation'
        assert count() == 1, f'expected one waiting-set claim, found {count()}'
        assert softcancel(first) is True
        assert softcancel(second) is True
        await asyncio.sleep(0.05)
        assert not client.aborted.is_set()
        assert count() == 1, f'expected one linger claim, found {count()}'
        # A third object with the same identity rejoins the lingering entry.
        third = expression('left')
        tasks.append(asyncio.create_task(third.compute_async(execution='local')))
        await asyncio.sleep(0.05)
        assert client.calls == 1 and not client.aborted.is_set()
        assert count() == 1, f'rejoining added a claim: {count()}'
    finally:
        await finish(client, tasks)
    for _ in range(100):
        if count() == 0:
            break
        await asyncio.sleep(0.01)
    assert count() == 0, 'the waiting-set claim survived completion'
    third._release_refholds()

asyncio.run(main())
seamless.close()
print('CONTRACT_OK')
"""

_NEUTRAL_BODY = """
from seamless import Cell

async def main():
    client = await setup()
    owner = Cell(checksum=checksum, celltype='plain')  # non-scratch owner
    assert not cache.is_scratch_ref(checksum)
    expr = expression('left')
    task = asyncio.create_task(expr.compute_async(execution='local'))
    try:
        await asyncio.wait_for(client.started.wait(), 5)
        assert not cache.is_scratch_ref(checksum), 'in-flight claim marked the input scratch'
    finally:
        await finish(client, [task])
    assert not cache.is_scratch_ref(checksum), 'the input stayed marked scratch'
    owner._release_refholds()

asyncio.run(main())
seamless.close()
print('CONTRACT_OK')
"""


def _run(body):
    result = subprocess.run(
        [sys.executable, "-c", _SETUP + "\n" + textwrap.dedent(body)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=Path(__file__).parent,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CONTRACT_OK" in result.stdout
    assert "seamless.references" not in result.stderr, result.stderr
    return result


def test_waiting_set_claim_lasts_through_the_linger_and_ends_at_abort():
    _run(_LINGER_BODY)


def test_waiting_set_holds_one_claim_per_expression_identity():
    _run(_IDENTITY_BODY)


@pytest.mark.xfail(
    strict=False,
    reason="checksum-reference-lifecycle.md §1 (Neutral claim): contract ahead of "
    "code: the waiting set's hold_input claims with scratch=True "
    "(seamless/checksum/expression.py) and leaves an owned, published input marked "
    "scratch",
)
def test_waiting_set_claim_is_neutral_about_scratch_status():
    _run(_NEUTRAL_BODY)
