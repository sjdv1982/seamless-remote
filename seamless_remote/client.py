"""Shared client utilities for Seamless remote clients."""

from __future__ import annotations

import asyncio
import threading
import weakref
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, TypeVar, cast

import aiohttp
from aiohttp import ClientConnectionError, ClientPayloadError

RETRYABLE_EXCEPTIONS = (
    ClientConnectionError,
    ClientPayloadError,
    asyncio.TimeoutError,
    FileNotFoundError,
)

_clients = weakref.WeakSet()

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def _retry_operation(method: F) -> F:
    """Decorator that retries transient failures after reinitializing the client."""

    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        for attempt in range(5):
            try:
                await self._wait_for_init()
                return await method(self, *args, **kwargs)
            except RETRYABLE_EXCEPTIONS:
                self._initialized = False
                if attempt == 4:
                    raise
        raise RuntimeError("Unreachable")

    return cast(F, wrapper)


class Client:
    """Base client with shared initialization and session handling logic."""

    readonly: bool
    url: Optional[str] = None
    directory: Optional[str] = None
    _shutdown = False

    def __init__(self, readonly: bool):
        self.readonly = readonly
        self._initialized = False
        self._sessions = weakref.WeakKeyDictionary()
        _clients.add(self)

    async def _init(self):
        """Subclasses should override with initialization logic."""

    def _validate_init(self):
        """Subclasses can override to validate configuration after initialization."""

    async def _wait_for_init(self, *, skip_healthcheck: bool = False):
        if self._initialized:
            return
        await self._init()
        self._validate_init()
        if not skip_healthcheck and not self._shutdown:
            await self.healthcheck()
        self._initialized = True

    def ensure_initialized_sync(self, *, skip_healthcheck: bool = False):
        """Synchronously ensure initialization."""
        if self._initialized:
            return

        async def _do():
            await self._wait_for_init(skip_healthcheck=skip_healthcheck)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_do())
            finally:
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    pass
                loop.close()
            return

        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_do(), loop)
            return future.result()

        loop.run_until_complete(_do())

    def _require_url(self) -> str:
        if self.url is None:
            raise ValueError("No url configured for this client")
        return self.url

    def _get_session(self) -> aiohttp.ClientSession:
        self._require_url()
        thread = threading.current_thread()
        session_async = self._sessions.get(thread)
        if session_async is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop != session_async._loop:
                    session_async = None
            except RuntimeError:
                session_async = None
        if session_async is None:
            timeout = aiohttp.ClientTimeout(total=10)
            session_async = aiohttp.ClientSession(timeout=timeout)
            self._sessions[thread] = session_async
        return session_async

    async def healthcheck(self) -> None:
        """Verify that the remote server responds successfully to /healthcheck."""
        if self.url is None:
            return
        session_async = self._get_session()
        path = self._require_url() + "/healthcheck"
        try:
            async with session_async.get(path) as response:
                if not (200 <= response.status < 300):
                    text = await response.text()
                    raise RuntimeError(
                        f"Healthcheck failed for {self.url}: HTTP {response.status} {text}"
                    )
        except ClientConnectionError as exc:
            raise RuntimeError(f"Healthcheck failed for {self.url}: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Healthcheck failed for {self.url}: {exc}") from exc

    def _close_sessions(self):
        sessions = list(self._sessions.values())
        self._sessions.clear()
        for session in sessions:
            _close_session(session)

    def _close_sessions_for_thread(self, thread: threading.Thread):
        """Close the session that belongs to the provided thread, if any."""
        session = self._sessions.pop(thread, None)
        if session is not None:
            _close_session(session)


def close_all_clients():
    """Close all tracked API clients."""
    for client in list(_clients):
        client._close_sessions()


def _close_session(session: aiohttp.ClientSession) -> None:
    """Close a session on the event loop it belongs to."""
    loop = getattr(session, "_loop", None)
    if loop is None or loop.is_closed():
        _finalize_session_without_loop(session)
        return

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if loop is running_loop:
        coro = session.close()
        if loop.is_running():
            loop.create_task(coro)
        else:
            loop.run_until_complete(coro)
        return

    if loop.is_running():
        coro = session.close()
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            fut.result()
        except Exception:
            pass
        return

    thread_id = getattr(loop, "_thread_id", None)
    if thread_id == threading.get_ident():
        coro = session.close()
        loop.run_until_complete(coro)
        return
    # Loop is stopped and owned by another thread; try best-effort close
    _finalize_session_without_loop(session)


def _finalize_session_without_loop(session: aiohttp.ClientSession) -> None:
    """Best-effort close when the originating loop has already stopped."""
    connector = getattr(session, "_connector", None)
    if connector is not None:
        close = getattr(connector, "_close", None)
        if close is not None:
            try:
                close()
            except Exception:
                pass
    detach = getattr(session, "detach", None)
    if detach is not None:
        try:
            detach()
        except Exception:
            pass


__all__ = [
    "Client",
    "RETRYABLE_EXCEPTIONS",
    "_retry_operation",
    "_close_session",
    "_finalize_session_without_loop",
    "close_all_clients",
]
