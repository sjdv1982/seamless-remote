import asyncio
import sys

import pytest

from seamless import Buffer, CacheMissError, Checksum, Expression
from seamless.checksum import expression as expression_mod
from seamless.checksum.expression import (
    _active_expressions,
    evaluate_expression_remote,
    get_expression_cache,
)
from seamless.checksum.hash_type import HashType, get_hash_type_cache
from helpers.fake_remotes import _drop_buffer, _install_fake_remotes


def test_remote_expression_dispatch_writes_expression_cache(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Checksum("1" * 64)
    result_checksum = Buffer("remote", "str").get_checksum()
    expression_rows = {}
    calls = []
    key = (source_checksum.hex(), "a", "plain", "str")
    _install_fake_remotes(
        monkeypatch,
        expression_rows,
        {key: result_checksum.hex()},
        calls,
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="remote",
        )
    )

    assert result == result_checksum
    assert expression_rows[key] == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "jobserver:run", "database:set"]


def test_database_expression_cache_hit_precedes_auto_location(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Checksum("2" * 64)
    result_checksum = Buffer("cached", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {key: result_checksum}
    calls = []
    _install_fake_remotes(monkeypatch, expression_rows, {}, calls)
    monkeypatch.setattr(
        expression_mod,
        "choose_expression_evaluation_location",
        lambda *args: pytest.fail("a database hit must precede locality selection"),
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get"]


def test_memory_expression_cache_hit_precedes_remote_lookups(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Checksum("a" * 64)
    result_checksum = Buffer("memory cached", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    get_expression_cache()[key] = result_checksum
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    monkeypatch.setattr(
        expression_mod,
        "choose_expression_evaluation_location",
        lambda *args: pytest.fail("an expression-cache hit must precede locality selection"),
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == []


def test_auto_expression_uses_local_buffer_before_remote_dispatch(monkeypatch):
    get_expression_cache().clear()
    source = Buffer({"a": "local"}, "plain")
    source_checksum = source.get_checksum()
    source_ref = source.tempref()
    result_checksum = Buffer("local", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    _install_fake_remotes(monkeypatch, expression_rows, {key: result_checksum.hex()}, calls)

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "database:set"]
    source_ref.clear()


def test_auto_buffer_free_conversion_stays_local_without_input_buffer(monkeypatch):
    get_expression_cache().clear()
    source = Buffer(True, "bool")
    source_checksum = source.get_checksum()
    _drop_buffer(source_checksum)
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "",
            "bool",
            "int",
            execution="auto",
        )
    )

    assert result == Buffer(1, "int").get_checksum()
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "database:set"]


def test_auto_expression_dispatches_when_input_is_only_on_hashserver(monkeypatch):
    get_expression_cache().clear()
    source = Buffer({"a": "remote"}, "plain")
    source_checksum = source.get_checksum()
    source_content = source.content
    result_checksum = Buffer("remote", "str").get_checksum()
    _drop_buffer(source_checksum)
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    _install_fake_remotes(
        monkeypatch,
        expression_rows,
        {key: result_checksum.hex()},
        calls,
        buffers={source_checksum: source_content},
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "jobserver:run", "database:set"]


def test_auto_expression_without_jobserver_resolves_input_locally(monkeypatch):
    get_expression_cache().clear()
    source = Buffer({"a": "fallback"}, "plain")
    source_checksum = source.get_checksum()
    source_content = source.content
    result_checksum = Buffer("fallback", "str").get_checksum()
    _drop_buffer(source_checksum)
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    _install_fake_remotes(
        monkeypatch,
        expression_rows,
        {},
        calls,
        buffers={source_checksum: source_content},
        jobserver_available=False,
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "hashserver:get", "database:set"]


@pytest.mark.parametrize(
    "entrypoint",
    ["compute", "compute_async", "_evaluate_internal", "_evaluate_internal_async"],
)
def test_standalone_expression_evaluation_defaults_to_auto(monkeypatch, entrypoint):
    get_expression_cache().clear()
    source = Buffer({"a": "remote"}, "plain")
    source_checksum = source.get_checksum()
    source_content = source.content
    result_checksum = Buffer("remote", "str").get_checksum()
    _drop_buffer(source_checksum)
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    _install_fake_remotes(
        monkeypatch,
        expression_rows,
        {key: result_checksum.hex()},
        calls,
        buffers={source_checksum: source_content},
    )

    expr = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
    evaluate = getattr(expr, entrypoint)
    result = asyncio.run(evaluate()) if entrypoint.endswith("async") else evaluate()

    assert result == result_checksum
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "jobserver:run", "database:set"]


def test_expression_cancel_fast_path_returns_false():
    get_expression_cache().clear()
    source_checksum = Buffer({"a": "local"}, "plain").get_checksum()
    expression = Expression(source_checksum, "a", input_celltype="plain", celltype="str")

    assert expression.cancel() is False


def test_remote_expression_members_share_one_active_request(monkeypatch):
    get_expression_cache().clear()
    _active_expressions.clear()
    source_checksum = Checksum("3" * 64)
    result_checksum = Buffer("remote", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()
    _install_fake_remotes(monkeypatch, expression_rows, {key: result_checksum.hex()}, calls)

    from seamless_remote import jobserver_remote

    async def run_expression(
        input_checksum, path, celltype, target_celltype, *, scratch=True
    ):
        calls.append("jobserver:run")
        started.set()
        await release.wait()
        return result_checksum

    jobserver_remote.run_expression = run_expression
    expr1 = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
    expr2 = Expression(source_checksum, "a", input_celltype="plain", celltype="str")

    async def main():
        task1 = asyncio.create_task(expr1.compute_async(execution="remote"))
        await started.wait()
        task2 = asyncio.create_task(expr2.compute_async(execution="remote"))
        for _ in range(100):
            active = _active_expressions.get(key)
            if active is not None and len(active.members) == 2:
                break
            await asyncio.sleep(0.01)
        assert len(_active_expressions[key].members) == 2
        assert expr1.cancel() is True
        assert len(_active_expressions[key].members) == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task1
        assert await task2 == result_checksum

    asyncio.run(main())
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "jobserver:run", "database:get", "database:set"]


def test_remote_expression_last_member_softcancel_lingers_active_request(monkeypatch):
    get_expression_cache().clear()
    _active_expressions.clear()
    source_checksum = Checksum("4" * 64)
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    started = asyncio.Event()
    request_cancelled = asyncio.Event()
    _install_fake_remotes(monkeypatch, expression_rows, {key: "5" * 64}, calls)

    from seamless_remote import jobserver_remote

    async def run_expression(input_checksum, path, celltype, target_celltype, *, scratch):
        assert scratch is True
        calls.append("jobserver:run")
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            request_cancelled.set()
            raise

    jobserver_remote.run_expression = run_expression
    expression = Expression(source_checksum, "a", input_celltype="plain", celltype="str")

    async def main():
        task = asyncio.create_task(expression.compute_async(execution="remote"))
        await started.wait()
        softcancel = getattr(type(expression), "softcancel", None)
        if softcancel is None:
            assert expression.cancel() is True
        else:
            assert softcancel(expression) is True
        with pytest.raises(asyncio.CancelledError):
            await task

        # Leaving the waiting set abandons this caller immediately, but the
        # materialization site owns the fetch through its linger.
        await asyncio.sleep(0.05)
        assert not request_cancelled.is_set()

    asyncio.run(main())
    assert [call for call in calls if call != "database:set_hash_type"] == ["database:get", "jobserver:run"]


@pytest.mark.parametrize("execution", ["auto", "remote"])
def test_missing_jobserver_failure_policy(monkeypatch, execution):
    get_expression_cache().clear()
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls, jobserver_available=False)
    error = CacheMissError if execution == "auto" else RuntimeError
    with pytest.raises(error):
        asyncio.run(evaluate_expression_remote(
            Checksum("f" * 64), "a", "plain", "str", execution=execution
        ))
    assert "jobserver:run" not in calls
    assert ("hashserver:get" in calls) == (execution == "auto")


def test_auto_does_not_fallback_after_jobserver_failure(monkeypatch):
    get_expression_cache().clear()
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    from seamless_remote import jobserver_remote

    async def fail(*args, **kwargs):
        raise ConnectionError("configured jobserver failed")

    monkeypatch.setattr(jobserver_remote, "run_expression", fail)
    with pytest.raises(ConnectionError, match="configured jobserver failed"):
        asyncio.run(evaluate_expression_remote(Checksum("e" * 64), "a", "plain", "str"))
    assert "hashserver:get" not in calls


def test_memory_local_compute_inside_running_loop():
    get_expression_cache().clear()
    source = Buffer({"a": "inside loop"}, "plain")
    source_ref = source.tempref()

    async def compute():
        expr = Expression(source.get_checksum(), "a", input_celltype="plain", celltype="str")
        assert expr.compute() == Buffer("inside loop", "str").get_checksum()

    try:
        asyncio.run(compute())
    finally:
        source_ref.clear()


def test_nonlocal_compute_inside_running_loop_retains_bridge_error():
    get_expression_cache().clear()
    source = Buffer({"a": "outside process memory"}, "plain")
    source_checksum = source.get_checksum()
    _drop_buffer(source_checksum)

    async def compute():
        expr = Expression(source_checksum, "a", input_celltype="plain", celltype="str")
        with pytest.raises(
            RuntimeError,
            match="Cannot block on remote expression evaluation in a running loop",
        ):
            expr.compute()

    asyncio.run(compute())


def test_auto_without_remote_package_raises_cache_miss(monkeypatch):
    get_expression_cache().clear()
    monkeypatch.setitem(sys.modules, "seamless_remote", None)
    with pytest.raises(CacheMissError):
        asyncio.run(evaluate_expression_remote(Checksum("d" * 64), "a", "plain", "str"))


@pytest.mark.parametrize("execution", ["auto", "remote"])
def test_jobserver_cache_miss_propagates_without_fallback(monkeypatch, execution):
    get_expression_cache().clear()
    source_checksum = Checksum("c" * 64)
    calls = []
    _install_fake_remotes(monkeypatch, {}, {}, calls)
    from seamless_remote import jobserver_remote

    async def miss(input_checksum, *args, **kwargs):
        calls.append("jobserver:run")
        raise CacheMissError(input_checksum)

    monkeypatch.setattr(jobserver_remote, "run_expression", miss)
    with pytest.raises(CacheMissError) as info:
        asyncio.run(
            evaluate_expression_remote(
                source_checksum,
                "a",
                "plain",
                "str",
                execution=execution,
            )
        )

    assert info.value.checksum == source_checksum
    assert "hashserver:get" not in calls


def test_local_evaluation_stores_result_hash_type(monkeypatch):
    get_expression_cache().clear()
    get_hash_type_cache().clear()
    source = Buffer({"a": "classified result"}, "plain")
    source_checksum = source.get_checksum()
    source_ref = source.tempref()
    result_buffer = Buffer("classified result", "str")
    result_checksum = result_buffer.get_checksum()
    hash_type_rows = {}
    calls = []
    _install_fake_remotes(
        monkeypatch,
        {},
        {},
        calls,
        hash_type_rows=hash_type_rows,
    )

    get_hash_type_cache().pop(result_checksum, None)
    try:
        result = asyncio.run(
            evaluate_expression_remote(
                source_checksum,
                "a",
                "plain",
                "str",
                execution="local",
            )
        )
    finally:
        source_ref.clear()

    assert result == result_checksum
    from seamless.caching import buffer_writer
    buffer_writer.flush()
    assert hash_type_rows[result_checksum.hex()] == HashType.from_buffer(
        result_buffer
    ).word
