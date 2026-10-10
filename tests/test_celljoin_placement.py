"""Celljoin placement policy before remote dispatch is introduced."""
import asyncio
import sys
from pathlib import Path

import pytest
from seamless import Buffer, CacheMissError, Checksum
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
def test_selected_remote_refuses_until_dispatch_stage(monkeypatch, execution):
    spec, held, calls, _ = setup(monkeypatch, local=(False, False), remote=(True, True))
    with pytest.raises(NotImplementedError):
        evaluate(spec, execution)
    assert has_calls(calls) == [('has', joins.required_buffers(spec))]
    assert 'hashserver:get' not in calls


def test_invalid_execution_rejected(monkeypatch):
    spec, held, calls, _ = setup(monkeypatch)
    with pytest.raises(ValueError):
        evaluate(spec, 'elsewhere')
