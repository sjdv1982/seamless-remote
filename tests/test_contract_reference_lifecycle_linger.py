"""Contract test: contracts/internal/checksum-reference-lifecycle.md §10 — the
materialization waiting set holds a lifecycle claim on the input checksum for the
duration of the linger and releases it when the abort fires; the shutdown audit
stays quiet. (The page still calls this "design intent, not yet built"; the code
now has it, as the "expression materialization" role.)

Fresh interpreter, so the shutdown audit really audits this scenario only. No
external buffer service is needed: the read-server client is a local stub.
"""

import re
import subprocess
import sys
import textwrap
from pathlib import Path

_WAITER_TESTS = Path(__file__).with_name("test_materialization_waiter_contract.py")
_SETUP = re.search(r'_SETUP = """(.*?)"""', _WAITER_TESTS.read_text(), re.S).group(1)

_BODY = """
from seamless.reference_lifecycle import collect_refholder_claims

ROLE = 'expression materialization'

def roles():
    return [role for _holder, role in collect_refholder_claims().get(checksum, [])]

async def main():
    client = await setup()
    expr = expression('left')
    task = asyncio.create_task(expr.compute_async(execution='local'))
    try:
        await asyncio.wait_for(client.started.wait(), 5)
        assert roles().count(ROLE) == 1, roles()
        in_flight = cache.reference_snapshot()[checksum][0]
        assert softcancel(expr) is True
        await asyncio.sleep(0.05)
        assert not client.aborted.is_set()
        assert roles().count(ROLE) == 1, 'claim ended before the linger did'
        assert cache.reference_snapshot()[checksum][0] == in_flight
        await asyncio.wait_for(client.aborted.wait(), 10)
        for _ in range(100):
            if ROLE not in roles():
                break
            await asyncio.sleep(0.01)
        assert ROLE not in roles(), 'claim survived the abort'
        assert cache.reference_snapshot()[checksum][0] == in_flight - 1
    finally:
        await finish(client, [task])

asyncio.run(main())
seamless.close()
print('CONTRACT_OK')
"""


def test_waiting_set_claim_lasts_through_the_linger_and_ends_at_abort():
    result = subprocess.run(
        [sys.executable, "-c", _SETUP + "\n" + textwrap.dedent(_BODY)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=Path(__file__).parent,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CONTRACT_OK" in result.stdout
    assert "live claims" not in result.stderr, result.stderr
    assert "eviction bridge" not in result.stderr, result.stderr
