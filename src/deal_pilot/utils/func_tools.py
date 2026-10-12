import asyncio
import functools
import inspect
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

_MISSING = object()


def _valid(expires_at: float) -> bool:
    return time.monotonic() < expires_at


def _cache_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    return functools._make_key(args, kwargs, typed=False)


class _SyncEntry:
    __slots__ = (
        "value",
        "expires_at",
        "lock",
        "cond",
        "_refreshing",
        "inflight",
        "shared_error",
    )

    def __init__(self) -> None:
        self.value: Any = _MISSING
        self.expires_at: float = 0.0
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self._refreshing = False
        self.inflight = False
        self.shared_error: BaseException | None = None


class _AsyncEntry:
    __slots__ = (
        "value",
        "expires_at",
        "lock",
        "cond",
        "_refreshing",
        "inflight",
        "shared_error",
    )

    def __init__(self) -> None:
        self.value: Any = _MISSING
        self.expires_at: float = 0.0
        self.lock = asyncio.Lock()
        self.cond = asyncio.Condition(self.lock)
        self._refreshing = False
        self.inflight = False
        self.shared_error: BaseException | None = None


def func_cache(*, ttl: int = 120, lazy: bool = False):
    """按函数和参数缓存结果，支持同步函数与异步函数。

    同一参数在首次调用或到期刷新时只执行一次，其余调用等待同一结果。
    开启 ``lazy`` 后，到期且已有缓存时立即返回旧值，并在后台刷新。
    刷新失败时保留旧值；首次计算失败时将异常传给所有等待者。

    参数：
        ttl: 缓存有效时间，单位为秒，必须大于等于零。
        lazy: 到期时是否先返回旧值，再在后台刷新。
    """

    if ttl < 0:
        raise ValueError("ttl 不能小于零")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        cache: dict[Any, _SyncEntry | _AsyncEntry] = {}

        def entry_for(key: Any) -> _SyncEntry | _AsyncEntry:
            item = cache.get(key)
            if item is None:
                if inspect.iscoroutinefunction(func):
                    item = _AsyncEntry()
                else:
                    item = _SyncEntry()
                cache[key] = item
            return item

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key = _cache_key(args, kwargs)
                entry = entry_for(key)
                assert isinstance(entry, _AsyncEntry)

                if (
                    lazy
                    and entry.value is not _MISSING
                    and not _valid(entry.expires_at)
                ):
                    _schedule_async_refresh(entry, func, args, kwargs, ttl)
                    return entry.value  # type: ignore[return-value]

                async with entry.cond:
                    while True:
                        if entry.value is not _MISSING and _valid(entry.expires_at):
                            return entry.value  # type: ignore[return-value]
                        if (
                            entry.shared_error is not None
                            and entry.value is _MISSING
                        ):
                            raise entry.shared_error
                        if entry.inflight:
                            await entry.cond.wait()
                            continue
                        entry.inflight = True
                        entry.shared_error = None
                        break

                try:
                    result = await func(*args, **kwargs)
                except BaseException as exc:
                    async with entry.cond:
                        entry.inflight = False
                        if entry.value is _MISSING:
                            entry.shared_error = exc
                        entry.cond.notify_all()
                    raise
                else:
                    async with entry.cond:
                        entry.value = result
                        entry.expires_at = time.monotonic() + ttl
                        entry.inflight = False
                        entry.cond.notify_all()
                    return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = _cache_key(args, kwargs)
            entry = entry_for(key)
            assert isinstance(entry, _SyncEntry)

            if (
                lazy
                and entry.value is not _MISSING
                and not _valid(entry.expires_at)
            ):
                _schedule_sync_refresh(entry, func, args, kwargs, ttl)
                return entry.value  # type: ignore[return-value]

            with entry.cond:
                while True:
                    if entry.value is not _MISSING and _valid(entry.expires_at):
                        return entry.value  # type: ignore[return-value]
                    if (
                        entry.shared_error is not None
                        and entry.value is _MISSING
                    ):
                        raise entry.shared_error
                    if entry.inflight:
                        entry.cond.wait()
                        continue
                    entry.inflight = True
                    entry.shared_error = None
                    break

            try:
                result = func(*args, **kwargs)
            except BaseException as exc:
                with entry.cond:
                    entry.inflight = False
                    if entry.value is _MISSING:
                        entry.shared_error = exc
                    entry.cond.notify_all()
                raise
            else:
                with entry.cond:
                    entry.value = result
                    entry.expires_at = time.monotonic() + ttl
                    entry.inflight = False
                    entry.cond.notify_all()
                return result

        return sync_wrapper  # type: ignore[return-value]

    return decorator


def _schedule_sync_refresh(
    entry: _SyncEntry,
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    ttl: int,
) -> None:
    with entry.lock:
        if entry._refreshing:
            return
        entry._refreshing = True

    def run() -> None:
        try:
            with entry.lock:
                try:
                    result = func(*args, **kwargs)
                except Exception:
                    return
                entry.value = result
                entry.expires_at = time.monotonic() + ttl
        finally:
            entry._refreshing = False

    threading.Thread(target=run, daemon=True).start()


def _schedule_async_refresh(
    entry: _AsyncEntry,
    func: Callable[..., Awaitable[Any]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    ttl: int,
) -> None:
    # lazy 路径在锁外快速返回旧值，此处仅避免并发重复后台刷新
    if entry._refreshing:
        return
    entry._refreshing = True

    async def run() -> None:
        try:
            async with entry.cond:
                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    return
                entry.value = result
                entry.expires_at = time.monotonic() + ttl
        finally:
            entry._refreshing = False

    asyncio.create_task(run())
