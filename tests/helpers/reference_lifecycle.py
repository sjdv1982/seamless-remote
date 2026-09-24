"""Test helpers for exercising buffer-cache lifetime boundaries.

These are deliberately test-only cache controls. They make forced-expiry tests
independent of weak-cache luck, expression-result weak references, the small
checksum fallback cache, and remote/recomputation repair paths. The helper
never clears the refholder count or bridge: those are the behavior under test.
"""

from __future__ import annotations

import gc
import importlib
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from seamless.checksum.calculate_checksum import TRIVIAL_CHECKSUMS


@dataclass(frozen=True)
class ForcedExpiryResult:
    """Observable before/after state returned by :func:`force_expiry`."""

    before: dict[str, Any]
    after: dict[str, Any]


def cache_entry_state(cache: Any, checksum: Any) -> dict[str, Any]:
    """Return the observable state used by lifecycle tests."""

    with cache.lock:
        entry = cache.strong_cache.get(checksum)
        if entry is None:
            return {
                "strong": False,
                "manual_refs": 0,
                "tempref": None,
                "buffer": None,
            }
        return {
            "strong": True,
            "manual_refs": entry.manual_refs,
            "tempref": entry.tempref,
            "buffer": entry.buffer,
        }


def _snapshot(cache: Any, checksum: Any) -> dict[str, Any]:
    with cache.lock:
        entry = cache.strong_cache.get(checksum)
        return {
            "accounting": cache.reference_snapshot().get(checksum, (0, 0, False)),
            "strong": entry is not None,
            "strong_buffer": entry is not None and entry.buffer is not None,
            "weak": checksum in cache.weak_cache,
            "tempref": None if entry is None else entry.tempref,
        }


def _drop_known_fallbacks(checksum: Any) -> None:
    expression_module = importlib.import_module("seamless.checksum.expression")
    result_buffers = getattr(expression_module, "_expression_result_buffers", None)
    if result_buffers is not None:
        result_buffers.pop(checksum, None)
    expression_cache = getattr(expression_module, "_expression_cache", None)
    if expression_cache is not None:
        for key, value in list(expression_cache.items()):
            if value == checksum:
                expression_cache.pop(key, None)
    try:
        from seamless.checksum.cached_calculate_checksum import checksum_cache
    except Exception:
        checksum_cache = None
    if checksum_cache is not None:
        checksum_cache.pop(checksum, None)


def force_expiry(
    checksum: Any,
    *,
    cache: Any = None,
    drop_buffers: Iterable[Any] = (),
    cleanup_callbacks: Iterable[Callable[[], Any]] = (),
) -> ForcedExpiryResult:
    """Force a concrete checksum through the cache's expiry boundary.

    ``drop_buffers`` is consumed before collection; callers should also delete
    their local variables when they are no longer needed. ``cleanup_callbacks``
    removes component-specific mappings before eviction. Repair counters belong
    to the caller's monkeypatch guards and are not maintained by this helper.
    """

    if checksum.hex() in TRIVIAL_CHECKSUMS:
        raise AssertionError("forced-expiry checksums must not be trivial")

    if cache is None:
        from seamless.caching.buffer_cache import get_buffer_cache

        cache = get_buffer_cache()

    before = _snapshot(cache, checksum)
    for callback in cleanup_callbacks:
        callback()
    _drop_known_fallbacks(checksum)

    # Clear a mutable caller-owned collection when supplied; this is the only
    # reliable way for a helper to drop references held by its caller. For
    # generators, consume the values and let the temporary collection die.
    if hasattr(drop_buffers, "clear"):
        drop_buffers.clear()  # type: ignore[attr-defined]
    else:
        tuple(drop_buffers)
    with cache.lock:
        entry = cache.strong_cache.get(checksum)
        if entry is not None and entry.tempref is not None:
            entry.tempref.clear()
        cache.weak_cache.pop(checksum, None)
        old_soft_cap = cache.soft_cap
        old_hard_cap = cache.hard_cap
        cache.soft_cap = 0
        cache.hard_cap = 0

    try:
        cache.run_eviction_once()
        gc.collect()
        gc.collect()
    finally:
        cache.soft_cap = old_soft_cap
        cache.hard_cap = old_hard_cap
    return ForcedExpiryResult(before=before, after=_snapshot(cache, checksum))
