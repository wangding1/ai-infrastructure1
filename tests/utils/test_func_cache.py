"""func_cache 行为测试（仅依据装饰器文档描述，不绑定实现细节）。"""

import asyncio
import threading
import time

import pytest

from deal_pilot.utils.func_tools import func_cache


class TestFuncCacheSpec:
    def test_ttl_must_be_non_negative(self):
        with pytest.raises(ValueError):
            func_cache(ttl=-1)

    def test_sync_caches_result_for_same_args_within_ttl(self):
        calls: list[int] = []

        @func_cache(ttl=60)
        def f(x: int) -> int:
            calls.append(x)
            return x * 10

        assert f(3) == 30
        assert f(3) == 30
        assert calls == [3]

    def test_sync_different_args_execute_separately(self):
        calls: list[int] = []

        @func_cache(ttl=60)
        def f(x: int) -> int:
            calls.append(x)
            return x

        assert f(1) == 1
        assert f(2) == 2
        assert calls == [1, 2]

    def test_sync_recomputes_after_ttl_expires(self):
        calls: list[int] = []

        @func_cache(ttl=1)
        def f() -> int:
            calls.append(1)
            return len(calls)

        assert f() == 1
        assert f() == 1
        time.sleep(1.05)
        assert f() == 2

    def test_sync_concurrent_same_args_single_execution(self):
        """到期前：同参数并发调用应只执行一次，其余等待同一结果。"""
        start = threading.Event()
        allow_finish = threading.Event()
        calls = 0

        @func_cache(ttl=60)
        def f() -> str:
            nonlocal calls
            calls += 1
            start.set()
            allow_finish.wait(timeout=5)
            return "ok"

        results: list[str] = []

        def run() -> None:
            results.append(f())

        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        assert start.wait(timeout=5)
        allow_finish.set()
        for t in threads:
            t.join(timeout=5)

        assert sorted(results) == ["ok", "ok", "ok"]
        assert calls == 1

    def test_sync_first_failure_propagates_to_concurrent_waiters(self):
        """首次计算失败时，正在等待同一结果的调用者应收到同一异常。"""
        in_compute = threading.Event()
        release = threading.Event()
        calls = 0

        @func_cache(ttl=60)
        def f() -> None:
            nonlocal calls
            calls += 1
            in_compute.set()
            release.wait(timeout=5)
            raise RuntimeError("first compute failed")

        errors: list[BaseException] = []

        def run() -> None:
            try:
                f()
            except RuntimeError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        assert in_compute.wait(timeout=5)
        release.set()
        for t in threads:
            t.join(timeout=5)

        assert calls == 1
        assert len(errors) == 3
        assert all(str(e) == "first compute failed" for e in errors)

    def test_async_caches_within_ttl(self):
        calls: list[int] = []

        @func_cache(ttl=60)
        async def af(x: int) -> int:
            calls.append(x)
            return x + 1

        assert asyncio.run(af(5)) == 6
        assert asyncio.run(af(5)) == 6
        assert calls == [5]

    def test_async_concurrent_same_args_single_execution(self):
        entered = 0
        lock = asyncio.Lock()
        release = asyncio.Event()

        @func_cache(ttl=60)
        async def af() -> str:
            nonlocal entered
            async with lock:
                entered += 1
            await release.wait()
            return "done"

        async def run_many() -> list[str]:
            release.set()
            return list(await asyncio.gather(af(), af(), af()))

        results = asyncio.run(run_many())
        assert results == ["done", "done", "done"]
        assert entered == 1

    def test_lazy_returns_stale_immediately_when_expired(self):
        refresh_started = threading.Event()
        allow_refresh_finish = threading.Event()
        calls = 0
        lock = threading.Lock()

        @func_cache(ttl=1, lazy=True)
        def f() -> int:
            nonlocal calls
            with lock:
                calls += 1
                n = calls
            if n > 1:
                refresh_started.set()
                allow_refresh_finish.wait(timeout=5)
            return n

        assert f() == 1
        time.sleep(1.05)
        t0 = time.monotonic()
        assert f() == 1
        assert time.monotonic() - t0 < 0.5
        assert refresh_started.wait(timeout=5)
        allow_refresh_finish.set()
        time.sleep(0.05)
        assert f() == 2

    def test_lazy_refresh_failure_keeps_stale_value(self):
        calls = 0
        lock = threading.Lock()

        @func_cache(ttl=1, lazy=True)
        def f() -> int:
            nonlocal calls
            with lock:
                calls += 1
                n = calls
            if n > 1:
                raise OSError("refresh failed")
            return n

        assert f() == 1
        time.sleep(1.05)
        assert f() == 1
        time.sleep(0.2)
        assert f() == 1
        with lock:
            assert calls >= 2
