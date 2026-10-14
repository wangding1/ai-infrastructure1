"""跨 tests 子目录共享的测试辅助函数。"""

import asyncio
from contextlib import contextmanager

from langchain_core.runnables.config import var_child_runnable_config


def run_async(coro):
    return asyncio.run(coro)


@contextmanager
def langgraph_user_context(user_id: str):
    token = var_child_runnable_config.set(
        {"configurable": {"langgraph_auth_user_id": user_id}}
    )
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)
