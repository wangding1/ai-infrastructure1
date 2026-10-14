"""Deploy 集成测试：StateGraph + ToolNode 调用与沙箱/桶隔离。"""

import json
import os
import uuid
from contextvars import ContextVar
from datetime import timedelta
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.store.memory import InMemoryStore

from deal_pilot.config.nacos.nacos_schemas import OpenSandboxConfig
import importlib

deploy_module = importlib.import_module("deal_pilot.tools.deploy")
from deal_pilot.utils.minio import MinIO
from helpers import make_minio_client, run_async

DEPLOY_WORKSPACE = "/tmp/deploy-integration/workspace"

_host_upload_guard: ContextVar[bool] = ContextVar("deploy_host_upload_guard", default=False)


def host_upload_guard_enabled() -> bool:
    return _host_upload_guard.get()


def set_host_upload_guard(enabled: bool):
    return _host_upload_guard.set(enabled)


def reset_host_upload_guard(token) -> None:
    _host_upload_guard.reset(token)


def _install_host_upload_guard() -> None:
    if getattr(MinIO, "_deploy_guard_installed", False):
        return
    original_upload_file = MinIO.upload_file
    original_upload_dir = MinIO.upload_dir

    async def guarded_upload_file(self, **kwargs: Any) -> None:
        if _host_upload_guard.get():
            raise AssertionError("禁止通过宿主 MinIO SDK 上传部署产物")
        return await original_upload_file(self, **kwargs)

    async def guarded_upload_dir(self, **kwargs: Any) -> Any:
        if _host_upload_guard.get():
            raise AssertionError("禁止通过宿主 MinIO SDK 上传部署产物")
        return await original_upload_dir(self, **kwargs)

    MinIO.upload_file = guarded_upload_file
    MinIO.upload_dir = guarded_upload_dir
    MinIO._deploy_guard_installed = True


_install_host_upload_guard()


async def invoke_deploy_via_graph(
    *,
    store: InMemoryStore,
    user_id: str,
    path: str,
    target_dirname: str,
    entry_files: list[str],
) -> dict[str, Any]:
    builder = StateGraph(MessagesState)
    builder.add_node("tools", ToolNode([deploy_module.deploy]))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    graph = builder.compile(store=store)

    call_id = f"deploy-{uuid.uuid4().hex[:8]}"
    message = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "deploy",
                "args": {
                    "path": path,
                    "target_dirname": target_dirname,
                    "entry_files": entry_files,
                },
                "id": call_id,
                "type": "tool_call",
            }
        ],
    )
    config = {"configurable": {"langgraph_auth_user_id": user_id}}
    token = set_host_upload_guard(True)
    try:
        state = await graph.ainvoke({"messages": [message]}, config)
    finally:
        reset_host_upload_guard(token)

    tool_messages = [
        m
        for m in state["messages"]
        if isinstance(m, ToolMessage) and m.tool_call_id == call_id
    ]
    if not tool_messages:
        raise AssertionError("ToolNode 未返回 ToolMessage")
    tool_message = tool_messages[-1]
    if tool_message.status == "error":
        raise ValueError(str(tool_message.content))
    return json.loads(str(tool_message.content))


def sandbox_config_for_deploy(image: str) -> OpenSandboxConfig:
    return OpenSandboxConfig(
        image=image,
        timeout_seconds=600,
        ready_timeout_seconds=180,
        command_timeout_seconds=600,
    )


async def write_workspace_files(
    user_id: str,
    store: InMemoryStore,
    files: dict[str, bytes],
) -> None:
    from pathlib import Path

    from deal_pilot.utils.sandbox import enter_sandbox

    parent_dirs = sorted(
        {str(Path(path).parent) for path in files if "/" in path},
        key=len,
    )
    async with enter_sandbox(user_id, store=store) as sandbox:
        await sandbox.commands.run(f"mkdir -p -- {DEPLOY_WORKSPACE}")
        if parent_dirs:
            await sandbox.commands.run(f"mkdir -p -- {' '.join(parent_dirs)}")
        for path, data in files.items():
            await sandbox.files.write_file(path, data)


async def remove_workspace_paths(
    user_id: str, store: InMemoryStore, paths: list[str]
) -> None:
    if not paths:
        return
    from deal_pilot.utils.sandbox import enter_sandbox

    async with enter_sandbox(user_id, store=store) as sandbox:
        await sandbox.commands.run(f"rm -f -- {' '.join(paths)}")


async def terminate_user_sandbox(user_id: str, store: InMemoryStore) -> None:
    from opensandbox import Sandbox
    from opensandbox.config import ConnectionConfig

    from deal_pilot.config.sandbox_settings import sandbox_settings
    from deal_pilot.utils.sandbox import _NAMESPACE, _get_store

    sandbox_id = await _get_store(store, user_id, "sandbox_id")
    if not sandbox_id:
        return
    connection_config = ConnectionConfig(
        domain=sandbox_settings.domain,
        api_key=sandbox_settings.api_key,
        use_server_proxy=True,
    )
    sandbox = await Sandbox.connect(
        sandbox_id=sandbox_id, connection_config=connection_config
    )
    await sandbox.kill()


async def delete_test_bucket(bucket_name: str) -> None:
    client = make_minio_client()
    if await client.bucket_exists(bucket_name):
        await client.remove_bucket(bucket_name, force=True)


def new_test_bucket_name() -> str:
    return f"minio-it-{uuid.uuid4().hex[:12]}"


def invoke_deploy(
    store: InMemoryStore,
    user_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    return run_async(invoke_deploy_via_graph(store=store, user_id=user_id, **kwargs))
