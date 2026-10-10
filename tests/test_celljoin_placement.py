"""Celljoin placement policy before remote dispatch is introduced."""
import asyncio
import sys
from pathlib import Path

import pytest
from seamless import Buffer, CacheMissError, Checksum
from seamless.checksum.expression import ExpressionEvaluationError
from seamless.checksum import celljoin as joins
from seamless.checksum import expression as expressions

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers.fake_remotes import drop_buffer, install_fake_remotes


@pytest.fixture(autouse=True)
def isolated_cache():
    expressions.get_expression_cache().clear()
    yield
    expressions.get_expression_cache().clear()


def setup(monkeypatch, *, local=(True, True), remote=(False, False), backend=True,
          writable=True, deep=False, root=True):
    celltype = 'deepcell' if deep else 'plain'
    buffers = [Buffer({'kept': '22' * 32} if deep else {'kept': 1}, celltype),
               Buffer(7, 'plain')]
    for buffer in buffers:
        buffer.tempref()
    spec = joins.parse_celljoin(joins.build_celljoin(
        buffers[0].get_checksum() if root else None,
        {'a': buffers[1].get_checksum()}), celltype)
    calls, rows = [], {}
    content = {b.get_checksum(): b.content for b, present in zip(buffers, remote) if present}
    install_fake_remotes(monkeypatch, {}, {}, calls, buffers=content,
                         jobserver_available=backend)
    from seamless_remote import buffer_remote, database_remote
    async def get_result(checksum, celltype):
        calls.append(('database:get_celljoin', checksum, celltype))
        return rows.get((checksum, celltype))
    async def set_result(checksum, celltype, result):
        rows[checksum, celltype] = result
        return True
    async def has_buffers(checksums):
        checksums = tuple(checksums)
        calls.append(('has', checksums))
        return [cs in content for cs in checksums]
    database_remote.get_celljoin_result = get_result
    database_remote.set_celljoin_result = set_result
    database_remote.has_write_server = lambda: writable
    buffer_remote.has_write_server = lambda: True
    buffer_remote.has_buffers = has_buffers
    from seamless_remote import jobserver_remote
    async def run_celljoin(checksum, celltype, *, scratch=True):
        calls.append(('dispatch', checksum, celltype, scratch))
        return Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    async def write(self):
        calls.append(('write', self.get_checksum()))
        return True
    monkeypatch.setattr(Buffer, 'write', write)
    jobserver_remote.run_celljoin = run_celljoin
    for buffer, present in zip(buffers, local):
        if not present:
            drop_buffer(buffer.get_checksum())
    return spec, buffers, calls, rows


def evaluate(spec, execution='auto'):
    return asyncio.run(joins.evaluate_celljoin_placed(spec, execution=execution))


def has_calls(calls):
    return [call for call in calls if isinstance(call, tuple) and call[0] == 'has']


def test_all_local_skips_presence(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch)
    assert evaluate(spec) == Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    assert not has_calls(calls)
    assert 'hashserver:get' not in calls


def test_mixed_local_and_remote_fetches_only_missing(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(True, False), remote=(False, True))
    assert evaluate(spec) == Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    assert has_calls(calls) == [('has', joins.required_buffers(spec))]
    assert calls.count('hashserver:get') == 1


def test_missing_input_is_final_without_fingertip(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(True, False))
    def forbidden(*args, **kwargs):
        raise AssertionError('ordinary join evaluation must not fingertip')
    monkeypatch.setattr(Checksum, 'fingertip', forbidden)
    with pytest.raises(CacheMissError) as error:
        evaluate(spec)
    assert held[1].get_checksum().hex() in str(error.value)
    assert 'hashserver:get' not in calls


@pytest.mark.parametrize('backend,writable', [(False, True), (True, False)])
def test_auto_downgrades_to_local(monkeypatch, backend, writable):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True),
                                backend=backend, writable=writable)
    assert evaluate(spec) == Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    assert len(has_calls(calls)) == 1
    assert calls.count('hashserver:get') == 2


def test_explicit_remote_requires_writable_database(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True), writable=False)
    with pytest.raises(RuntimeError, match='requires a hashserver and a database'):
        evaluate(spec, 'remote')
    assert 'hashserver:get' not in calls


def test_explicit_remote_rejects_local_only_input(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, remote=(True, False))
    with pytest.raises(CacheMissError) as error:
        evaluate(spec, 'remote')
    assert held[1].get_checksum().hex() in str(error.value)
    assert 'hashserver:get' not in calls


def test_explicit_local_fetches_without_presence_or_dispatch(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    assert evaluate(spec, 'local') == Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    assert not has_calls(calls)
    assert calls.count('hashserver:get') == 2


@pytest.mark.parametrize('execution', ['auto', 'local', 'remote'])
@pytest.mark.parametrize('root', [False, True])
def test_deep_always_local_fetching_only_root(monkeypatch, execution, root):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, False),
                                deep=True, root=root, writable=False)
    expected = {'a': held[1].get_checksum().hex()}
    if root:
        expected['kept'] = '22' * 32
    assert evaluate(spec, execution) == Buffer(expected, 'deepcell').get_checksum()
    assert not has_calls(calls)
    assert calls.count('hashserver:get') == int(root)


@pytest.mark.parametrize('cache', ['process', 'database'])
def test_recorded_result_precedes_placement(monkeypatch, cache):
    spec, held, calls, rows = setup(monkeypatch, local=(False, False), writable=False)
    result = Checksum('ab' * 32)
    if cache == 'process':
        expressions.get_expression_cache()[joins.celljoin_cache_key(spec.checksum, spec.celltype)] = result
    else:
        rows[spec.checksum, spec.celltype] = result
    assert evaluate(spec, 'remote') == result
    assert not has_calls(calls)
    assert 'hashserver:get' not in calls
    if cache == 'process':
        assert calls == []


@pytest.mark.parametrize('execution', ['auto', 'remote'])
@pytest.mark.parametrize('scratch', [True, False])
def test_remote_dispatch_identity_scratch_and_definition_order(monkeypatch, execution, scratch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    result = asyncio.run(joins.evaluate_celljoin_placed(spec, execution=execution, scratch=scratch))
    assert result == Buffer({'kept': 1, 'a': 7}, 'plain').get_checksum()
    assert has_calls(calls) == [('has', joins.required_buffers(spec))]
    assert 'hashserver:get' not in calls
    write = ('write', spec.checksum)
    dispatch = ('dispatch', spec.checksum, spec.celltype, scratch)
    assert calls.index(write) < calls.index(dispatch)
    assert expressions.get_expression_cache()[joins.celljoin_cache_key(spec.checksum, spec.celltype)] == result


@pytest.mark.parametrize('failure', [False, RuntimeError('write failed')])
def test_definition_write_failure_prevents_dispatch(monkeypatch, failure):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    async def write(self):
        if isinstance(failure, Exception):
            raise failure
        return failure
    monkeypatch.setattr(Buffer, 'write', write)
    with pytest.raises(RuntimeError if isinstance(failure, Exception) else ExpressionEvaluationError):
        evaluate(spec, 'remote')
    assert not any(isinstance(c, tuple) and c[0] == 'dispatch' for c in calls)


def test_configured_dispatch_error_is_not_downgraded(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    from seamless_remote import jobserver_remote
    async def fail(*args, **kwargs):
        raise CacheMissError(spec.checksum)
    monkeypatch.setattr(jobserver_remote, 'run_celljoin', fail)
    with pytest.raises(CacheMissError) as error:
        evaluate(spec)
    assert spec.checksum.hex() in str(error.value)
    assert 'hashserver:get' not in calls
    assert joins.celljoin_cache_key(spec.checksum, spec.celltype) not in expressions.get_expression_cache()


def test_shared_remote_members_hold_claims_and_strongest_scratch(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    from seamless_remote import jobserver_remote
    async def main():
        writing, release_write = asyncio.Event(), asyncio.Event()
        dispatched, release_dispatch = asyncio.Event(), asyncio.Event()
        observations = []
        async def write(self):
            writing.set()
            await release_write.wait()
            return True
        async def dispatch(checksum, celltype, *, scratch):
            observations.append((checksum, celltype, scratch))
            dispatched.set()
            await release_dispatch.wait()
            return Checksum('cd' * 32)
        monkeypatch.setattr(Buffer, 'write', write)
        monkeypatch.setattr(jobserver_remote, 'run_celljoin', dispatch)
        first = asyncio.create_task(joins.evaluate_celljoin_placed(spec, member_id=101, scratch=True))
        await asyncio.wait_for(writing.wait(), 5)
        second = asyncio.create_task(joins.evaluate_celljoin_placed(spec, member_id=202, scratch=False))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        active = expressions._active_expressions[joins.celljoin_cache_key(spec.checksum, spec.celltype)]
        expected = set(joins.required_buffers(spec)) | {spec.checksum}
        assert {cs for cs, role in active._refheld_checksums()} == expected
        assert observations == []
        release_write.set()
        await asyncio.wait_for(dispatched.wait(), 5)
        assert observations == [(spec.checksum, spec.celltype, False)]
        assert expressions.softcancel_expression(joins.celljoin_cache_key(spec.checksum, spec.celltype), 101)
        with pytest.raises(asyncio.CancelledError):
            await first
        release_dispatch.set()
        assert await second == Checksum('cd' * 32)
    asyncio.run(main())


def test_invalid_execution_rejected(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch)
    with pytest.raises(ValueError):
        evaluate(spec, 'elsewhere')


def test_last_remote_member_lingers_and_rejoins_single_dispatch(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    from seamless_remote import jobserver_remote
    monkeypatch.setattr(expressions, '_EXPRESSION_LINGER', 0.3)
    async def main():
        started, release = asyncio.Event(), asyncio.Event()
        dispatches = []
        async def dispatch(*args, **kwargs):
            dispatches.append((args, kwargs))
            started.set()
            await release.wait()
            return Checksum('ce' * 32)
        monkeypatch.setattr(jobserver_remote, 'run_celljoin', dispatch)
        first = asyncio.create_task(joins.evaluate_celljoin_placed(spec, member_id=301))
        await asyncio.wait_for(started.wait(), 5)
        key = joins.celljoin_cache_key(spec.checksum, spec.celltype)
        active = expressions._active_expressions[key]
        assert expressions.softcancel_expression(key, 301)
        with pytest.raises(asyncio.CancelledError):
            await first
        assert key not in expressions._active_expressions
        assert expressions._lingering_expressions[key] is active
        second = asyncio.create_task(joins.evaluate_celljoin_placed(spec, member_id=302))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert expressions._active_expressions[key] is active
        assert len(dispatches) == 1
        release.set()
        assert await second == Checksum('ce' * 32)
    asyncio.run(main())


def test_completed_remote_linger_releases_claims_without_done_callback(monkeypatch):
    """Finalization owns cleanup even if shutdown prevents task callbacks."""
    import concurrent.futures

    spec, held, calls, _ = setup(monkeypatch)
    monkeypatch.setattr(expressions, '_EXPRESSION_LINGER', 30)
    key = joins.celljoin_cache_key(spec.checksum, spec.celltype)

    async def main():
        started, release = asyncio.Event(), asyncio.Event()
        active = expressions._ActiveExpression(
            result_future=concurrent.futures.Future(), task=None, members={401})
        claims = joins.required_buffers(spec) + (spec.checksum,)
        active.hold_inputs(claims)
        expected = Checksum('cf' * 32)
        recorded = []

        async def dispatch(request):
            assert request is active
            started.set()
            await release.wait()
            return expected

        # Intentionally attach no done callback: only the runner's finally
        # can clean up this completed request before the event loop closes.
        active.task = asyncio.create_task(
            expressions._execute_active_remote(key, active, dispatch, recorded.append))
        expressions._active_expressions[key] = active
        try:
            await asyncio.wait_for(started.wait(), 5)
            assert expressions.softcancel_expression(key, 401)
            await asyncio.sleep(0)
            assert expressions._lingering_expressions[key] is active
            assert set(active.input_claims) == set(claims)
            assert not active.task.done()
            release.set()
            await asyncio.wait_for(active.task, 5)
            assert active.result_future.result() == expected
            assert recorded == [expected]
            assert key not in expressions._active_expressions
            assert key not in expressions._lingering_expressions
            assert active.input_claims == ()
            assert active._refheld_checksums() == ()
            assert active.linger is None or active.linger.cancelled()
        finally:
            release.set()
            if not active.task.done():
                active.task.cancel()
                await asyncio.gather(active.task, return_exceptions=True)
            expressions._discard_active_expression(key, active)

    asyncio.run(main())
