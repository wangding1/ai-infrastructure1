"""Deploy 工具集成测试（DEPLOY_INTEGRATION=1）。"""

import os
import uuid

import httpx
import pytest

from deploy_harness import (
    DEPLOY_WORKSPACE,
    invoke_deploy,
    remove_workspace_paths,
    write_workspace_files,
)
from helpers import make_minio_client, run_async

pytest_plugins = ["deploy_conftest"]

pytestmark = pytest.mark.skipif(
    os.environ.get("DEPLOY_INTEGRATION") != "1",
    reason="设置 DEPLOY_INTEGRATION=1 并配置 OpenSandbox 与 DEPLOY_SANDBOX_IMAGE",
)


def _dist(project: str) -> str:
    return f"{DEPLOY_WORKSPACE}/{project}/dist"


class TestDeployDirectory:
    def test_directory_deploy_and_http_entries(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"vue-app-{uuid.uuid4().hex[:6]}"
        root = _dist(target)
        files = {
            f"{root}/index.html": b"<html>ok</html>",
            f"{root}/static/chart.png": b"\x89PNG\r\n\x1a\n",
            f"{root}/app.js": b"js",
            f"{root}/.hidden": b"h",
            f"{root}/nested/nested.txt": b"n",
        }
        run_async(write_workspace_files(user_id, store, files))
        result = invoke_deploy(
            store,
            user_id,
            path=root,
            target_dirname=target,
            entry_files=["index.html", "static/chart.png"],
        )
        assert result["failure_count"] == 0
        assert len(result["urls"]) == 2
        for url in result["urls"]:
            assert httpx.get(url, timeout=30.0).status_code == 200

    def test_directory_redeploy_clears_stale_remote_objects(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"vue-app-{uuid.uuid4().hex[:6]}"
        root = _dist(target)
        prefix = f"{user_id}/{target}"
        client = make_minio_client()

        run_async(
            write_workspace_files(
                user_id,
                store,
                {
                    f"{root}/index.html": b"v1",
                    f"{root}/stale.txt": b"old",
                },
            )
        )
        invoke_deploy(store, user_id, path=root, target_dirname=target, entry_files=["index.html"])
        run_async(remove_workspace_paths(user_id, store, [f"{root}/stale.txt"]))
        run_async(write_workspace_files(user_id, store, {f"{root}/index.html": b"v2"}))
        result = invoke_deploy(
            store, user_id, path=root, target_dirname=target, entry_files=["index.html"]
        )
        assert httpx.get(result["urls"][0], timeout=30.0).content == b"v2"
        assert not run_async(
            client.file_exists(
                bucket_name=deploy_bucket, filename=f"{prefix}/stale.txt"
            )
        )

    def test_empty_directory_is_tool_error(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"empty-{uuid.uuid4().hex[:6]}"
        root = _dist(target)
        run_async(write_workspace_files(user_id, store, {}))
        async def _mkdir() -> None:
            from deal_pilot.utils.sandbox import enter_sandbox

            async with enter_sandbox(user_id, store=store) as sandbox:
                await sandbox.commands.run(f"mkdir -p -- {root}")

        run_async(_mkdir())
        with pytest.raises(ValueError):
            invoke_deploy(
                store, user_id, path=root, target_dirname=target, entry_files=["index.html"]
            )


class TestDeploySingleFile:
    def test_single_file_deploy(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"pkg-{uuid.uuid4().hex[:6]}"
        zip_path = f"{DEPLOY_WORKSPACE}/{target}/reporter.zip"
        payload = b"PK\x03\x04zip"
        run_async(write_workspace_files(user_id, store, {zip_path: payload}))
        result = invoke_deploy(
            store,
            user_id,
            path=zip_path,
            target_dirname=target,
            entry_files=["reporter.zip"],
        )
        assert result["success_count"] == 1
        assert httpx.get(result["urls"][0], timeout=30.0).content == payload

    def test_single_file_preserves_other_objects_under_prefix(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"mix-{uuid.uuid4().hex[:6]}"
        prefix = f"{user_id}/{target}"
        client = make_minio_client()
        run_async(
            client.create_bucket(deploy_bucket, allow_public_read=True)
        )
        run_async(
            client.write_file(
                bucket_name=deploy_bucket,
                filename=f"{prefix}/keep.txt",
                stream=__import__("io").BytesIO(b"stay"),
            )
        )
        html_path = f"{DEPLOY_WORKSPACE}/{target}/index.html"
        run_async(write_workspace_files(user_id, store, {html_path: b"new"}))
        invoke_deploy(
            store,
            user_id,
            path=html_path,
            target_dirname=target,
            entry_files=["index.html"],
        )
        assert run_async(
            client.file_exists(bucket_name=deploy_bucket, filename=f"{prefix}/keep.txt")
        )
        assert run_async(
            client.file_exists(bucket_name=deploy_bucket, filename=f"{prefix}/index.html")
        )


class TestDeployValidation:
    def test_missing_path_tool_error(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        with pytest.raises(ValueError, match="不存在"):
            invoke_deploy(
                store,
                user_id,
                path=f"{DEPLOY_WORKSPACE}/missing/dist",
                target_dirname="missing",
                entry_files=["index.html"],
            )

    def test_missing_entry_tool_error(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        target = f"no-entry-{uuid.uuid4().hex[:6]}"
        root = _dist(target)
        run_async(write_workspace_files(user_id, store, {f"{root}/index.html": b"x"}))
        with pytest.raises(ValueError, match="entry_files"):
            invoke_deploy(
                store,
                user_id,
                path=root,
                target_dirname=target,
                entry_files=["ghost.html"],
            )

    def test_auth_user_missing_raises(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_store: object,
    ):
        from langgraph.prebuilt import ToolNode
        from langgraph.graph import END, START, MessagesState, StateGraph
        from langchain_core.messages import AIMessage
        from deal_pilot.tools.deploy import deploy

        builder = StateGraph(MessagesState)
        builder.add_node("tools", ToolNode([deploy]))
        builder.add_edge(START, "tools")
        builder.add_edge("tools", END)
        graph = builder.compile(store=deploy_store)
        msg = AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "deploy",
                    "args": {
                        "path": f"{DEPLOY_WORKSPACE}/x",
                        "target_dirname": "x",
                        "entry_files": ["a"],
                    },
                    "id": "1",
                    "type": "tool_call",
                }
            ],
        )

        async def _run() -> None:
            await graph.ainvoke({"messages": [msg]}, {})

        with pytest.raises(RuntimeError, match="langgraph_auth_user_id"):
            run_async(_run())


class TestDeployIsolation:
    def test_same_user_different_target_dirname(
        self,
        deploy_patched_sandbox: None,
        deploy_bucket: str,
        deploy_user: tuple[str, object],
    ):
        user_id, store = deploy_user
        a, b = f"app-a-{uuid.uuid4().hex[:4]}", f"app-b-{uuid.uuid4().hex[:4]}"
        run_async(
            write_workspace_files(
                user_id,
                store,
                {
                    f"{_dist(a)}/index.html": b"A",
                    f"{_dist(b)}/index.html": b"B",
                },
            )
        )
        ra = invoke_deploy(store, user_id, path=_dist(a), target_dirname=a, entry_files=["index.html"])
        rb = invoke_deploy(store, user_id, path=_dist(b), target_dirname=b, entry_files=["index.html"])
        assert httpx.get(ra["urls"][0], timeout=30.0).content == b"A"
        assert httpx.get(rb["urls"][0], timeout=30.0).content == b"B"
