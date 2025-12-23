"""Shared client utilities for Seamless remote clients."""

from __future__ import annotations

import asyncio
import threading
import weakref
import os
import traceback
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, TypeVar, cast

import aiohttp
from aiohttp import ClientConnectionError, ClientPayloadError
from seamless import is_worker, ensure_open

RETRYABLE_EXCEPTIONS = (
    ClientConnectionError,
    ClientPayloadError,
    asyncio.TimeoutError,
    FileNotFoundError,
)

_clients = weakref.WeakSet()
_keepalive_loop: Optional[asyncio.AbstractEventLoop] = None
_keepalive_thread: Optional[threading.Thread] = None
_keepalive_stop_event: Optional[asyncio.Event] = None
_keepalive_lock = threading.RLock()

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def _ensure_not_child() -> None:
    if is_worker():
        raise RuntimeError("Remote clients are unavailable inside child processes")
    ensure_open("remote client", mark_required=False)


def _retry_operation(method: F) -> F:
    """Decorator that retries transient failures after reinitializing the client."""

    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        _ensure_not_child()
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
        _ensure_not_child()
        self.readonly = readonly
        self._initialized = False
        # Keep strong references to sessions; they are closed explicitly in close_all_clients.
        self._sessions = {}
        _clients.add(self)
        _ensure_keepalive_worker()
        # Ensure sessions are not leaked if the client is GC'd before explicit cleanup.
        self._finalizer = weakref.finalize(self, Client._finalize, weakref.ref(self))

    @staticmethod
    def _finalize(ref: "weakref.ReferenceType[Client]") -> None:
        client = ref()
        if client is None:
            return
        try:
            client._shutdown = True
            client._close_sessions()
        except Exception:
            pass

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
                if (
                    loop != session_async._loop
                    or session_async.closed
                    or session_async._loop.is_closed()
                ):
                    self._sessions.pop(thread, None)
                    _close_session(session_async)
                    session_async = None
            except RuntimeError:
                self._sessions.pop(thread, None)
                _close_session(session_async)
                session_async = None
        if session_async is None:
            timeout = aiohttp.ClientTimeout(total=10)
            session_async = aiohttp.ClientSession(timeout=timeout)
            self._sessions[thread] = session_async
            try:
                setattr(session_async, "_seamless_owner", weakref.ref(self))
            except Exception:
                pass
            _log_session_event("created", self, session_async)
        return session_async

    async def healthcheck(self) -> None:
        """Verify that the remote server responds successfully to /healthcheck."""
        if self.url is None:
            return
        session_async = self._get_session()
        path = self._require_url() + "/healthcheck"
        from aiohttp import ClientTimeout

        try:
            async with session_async.get(path, timeout=ClientTimeout(10)) as response:
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
    """Close all tracked API clients and stop keepalive worker."""
    ensure_open("close remote clients")
    _stop_keepalive_worker()
    for client in list(_clients):
        client._close_sessions()


def _close_session(session: aiohttp.ClientSession) -> None:
    """Close a session on the event loop it belongs to."""
    owner_ref = getattr(session, "_seamless_owner", None)
    owner = owner_ref() if owner_ref is not None else None
    _log_session_event("close", owner, session)
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
            ### fut.result(timeout=0.05)
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


# --- keepalive worker --------------------------------------------------------
# Purposefully keeps remote clients warm by periodically hitting /healthcheck;
# success is irrelevant, it just prevents idle timeouts.
def _ensure_keepalive_worker() -> None:
    """Ensure background keepalive thread is running."""
    global _keepalive_thread, _keepalive_loop, _keepalive_stop_event
    with _keepalive_lock:
        thread = _keepalive_thread
        if thread is not None and thread.is_alive():
            return
        worker = threading.Thread(
            target=_keepalive_thread_main,
            name="SeamlessRemoteClientKeepalive",
            daemon=True,
        )
        _keepalive_thread = worker
    worker.start()


def _stop_keepalive_worker() -> None:
    global _keepalive_thread, _keepalive_loop, _keepalive_stop_event
    thread = None
    with _keepalive_lock:
        loop = _keepalive_loop
        stop_event = _keepalive_stop_event
        thread = _keepalive_thread
    if loop is None or stop_event is None or thread is None:
        return

    def _request_stop() -> None:
        if not stop_event.is_set():
            stop_event.set()

    loop.call_soon_threadsafe(_request_stop)
    thread.join()
    for client in list(_clients):
        client._close_sessions_for_thread(thread)
    with _keepalive_lock:
        _keepalive_thread = None
        _keepalive_loop = None
        _keepalive_stop_event = None


async def _run_keepalive(stop_event: asyncio.Event) -> None:
    """Periodically send healthchecks to keep remote clients alive."""
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
            break
        except asyncio.TimeoutError:
            await _perform_healthchecks()


async def _perform_healthchecks() -> None:
    """Send keepalive healthchecks; errors are ignored by design."""
    tasks = []
    client: Client
    for client in list(_clients):
        try:
            assert isinstance(client, Client)
        except AssertionError:
            continue
        if not client._initialized:
            continue
        if client._shutdown:
            continue
        tasks.append(asyncio.create_task(_ping_client(client)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _ping_client(client: Client) -> None:
    try:
        await client._wait_for_init(skip_healthcheck=True)
        await client.healthcheck()
    except Exception:
        # Intentionally swallow all errors; the goal is to keep connections warm.
        pass


def _keepalive_thread_main() -> None:
    """Thread entry point running the keepalive loop."""
    global _keepalive_loop, _keepalive_thread, _keepalive_stop_event
    loop = asyncio.new_event_loop()
    stop_event = asyncio.Event()
    try:
        asyncio.set_event_loop(loop)
        with _keepalive_lock:
            _keepalive_loop = loop
            _keepalive_stop_event = stop_event
        loop.run_until_complete(_run_keepalive(stop_event))
    finally:
        asyncio.set_event_loop(None)
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        try:
            loop.run_until_complete(loop.shutdown_default_executor())
        except Exception:
            pass
        loop.close()
        with _keepalive_lock:
            _keepalive_loop = None
            _keepalive_thread = None
            _keepalive_stop_event = None


__all__ = [
    "Client",
    "RETRYABLE_EXCEPTIONS",
    "_retry_operation",
    "_close_session",
    "_finalize_session_without_loop",
    "close_all_clients",
]
_CLIENT_DEBUG = os.environ.get("SEAMLESS_CLIENT_DEBUG", "").lower() in {
    "1",
    "true",
    "yes",
}


def _log_session_event(
    event: str, client: "Client | None", session: aiohttp.ClientSession
) -> None:
    if not _CLIENT_DEBUG:
        return
    try:
        thread = threading.current_thread()
        stack = "".join(traceback.format_stack(limit=5))
        owner = client.__class__.__name__ if client is not None else "UnknownClient"
        print(
            f"[pid={os.getpid()}][{event}] {owner} "
            f"thread={thread.name} session={id(session)}",
            flush=True,
        )
        print(stack, flush=True)
    except Exception:
        pass
