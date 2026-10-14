"""deploy 工具集成测试（沙箱上传 + MinIO，需 DEPLOY_INTEGRATION=1）。"""

import os
import uuid

import httpx
import pytest

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.tools.deploy import deploy
from helpers import (
    langgraph_user_context,
    make_minio_client,
    remove_object_prefix,
    remove_sandbox_paths,
    run_async,
    write_sandbox_files,
)
pytestmark = pytest.mark.skipif(
    os.environ.get("DEPLOY_INTEGRATION") != "1",
    reason="设置 DEPLOY_INTEGRATION=1 并启动 shared-minio 与 OpenSandbox（同时需 MINIO_INTEGRATION=1）",
)


def _bucket_name() -> str:
    return minio_settings.bucket.strip() or "deal-pilot"


def _invoke_deploy(
    user_id: str,
    *,
    path: str,
    target_dirname: str,
    entry_files: list[str],
) -> dict:
    async def _run() -> dict:
        with langgraph_user_context(user_id):
            return await deploy.ainvoke(
                {
                    "path": path,
                    "target_dirname": target_dirname,
                    "entry_files": entry_files,
                }
            )

    return run_async(_run())


class TestDeployToolHappyPath:
    def test_directory_deploy_matches_doc_example(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"vue-app-{uuid.uuid4().hex[:8]}"
        dist_root = "/root/workspace/vue-app/dist"
        files = {
            f"{dist_root}/index.html": b"<html>deploy-it</html>",
            f"{dist_root}/static/chart.png": b"\x89PNG\r\n\x1a\n",
            f"{dist_root}/app.js": b"console.log('ok');",
            f"{dist_root}/style.css": b"body {}",
            f"{dist_root}/asset.json": b'{"v":1}',
        }
        entry_files = ["index.html", "static/chart.png"]
        bucket = _bucket_name()
        client = make_minio_client()
        prefix = f"{user_id}/{target_dirname}"

        run_async(write_sandbox_files(user_id, files))
        result = _invoke_deploy(
            user_id,
            path=dist_root,
            target_dirname=target_dirname,
            entry_files=entry_files,
        )
        try:
            assert result["total_count"] == len(files)
            assert result["success_count"] == len(files)
            assert result["failure_count"] == 0
            assert result["failures"] == {}
            assert len(result["urls"]) == len(entry_files)

            for entry, url in zip(entry_files, result["urls"], strict=True):
                assert f"/{user_id}/{target_dirname}/{entry}" in url
                response = httpx.get(url, timeout=30.0)
                assert response.status_code == 200
                assert response.content == files[f"{dist_root}/{entry}"]

            async def _assert_objects() -> None:
                for rel in files:
                    key = f"{prefix}/{rel.removeprefix(dist_root + '/')}"
                    assert await client.file_exists(bucket_name=bucket, filename=key)

            run_async(_assert_objects())
        finally:
            run_async(remove_object_prefix(client, bucket, prefix))

    def test_single_file_deploy_matches_doc_example(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"deep-research-{uuid.uuid4().hex[:8]}"
        zip_path = "/root/workspace/deep-research/dist/reporter.zip"
        payload = b"PK\x03\x04fake-zip-content"
        bucket = _bucket_name()
        client = make_minio_client()
        prefix = f"{user_id}/{target_dirname}"

        run_async(write_sandbox_files(user_id, {zip_path: payload}))
        result = _invoke_deploy(
            user_id,
            path=zip_path,
            target_dirname=target_dirname,
            entry_files=["reporter.zip"],
        )
        try:
            assert result["total_count"] == 1
            assert result["success_count"] == 1
            assert result["failure_count"] == 0
            assert result["failures"] == {}
            assert len(result["urls"]) == 1
            url = result["urls"][0]
            assert f"/{user_id}/{target_dirname}/reporter.zip" in url
            response = httpx.get(url, timeout=30.0)
            assert response.status_code == 200
            assert response.content == payload
        finally:
            run_async(remove_object_prefix(client, bucket, prefix))

    def test_entry_files_with_leading_slash_normalized(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"vue-app-{uuid.uuid4().hex[:8]}"
        dist_root = "/root/workspace/vue-app/dist"
        html = b"<html>slash</html>"
        bucket = _bucket_name()
        client = make_minio_client()
        prefix = f"{user_id}/{target_dirname}"

        run_async(
            write_sandbox_files(user_id, {f"{dist_root}/index.html": html})
        )
        result = _invoke_deploy(
            user_id,
            path=dist_root,
            target_dirname=target_dirname,
            entry_files=["/index.html"],
        )
        try:
            assert result["total_count"] == 1
            assert result["failure_count"] == 0
            url = result["urls"][0]
            assert f"/{user_id}/{target_dirname}/index.html" in url
            assert httpx.get(url, timeout=30.0).content == html
        finally:
            run_async(remove_object_prefix(client, bucket, prefix))

    def test_static_assets_get_long_cache_control(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"vue-app-{uuid.uuid4().hex[:8]}"
        dist_root = "/root/workspace/vue-app/dist"
        bucket = _bucket_name()
        client = make_minio_client()
        prefix = f"{user_id}/{target_dirname}"

        run_async(
            write_sandbox_files(
                user_id,
                {f"{dist_root}/index.html": b"<html></html>"},
            )
        )
        result = _invoke_deploy(
            user_id,
            path=dist_root,
            target_dirname=target_dirname,
            entry_files=["index.html"],
        )
        try:
            response = httpx.get(result["urls"][0], timeout=30.0)
            cache = response.headers.get("cache-control", "")
            assert "max-age=7776000" in cache
        finally:
            run_async(remove_object_prefix(client, bucket, prefix))


class TestDeployToolReplace:
    def test_directory_redeploy_removes_stale_objects(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"vue-app-{uuid.uuid4().hex[:8]}"
        dist_root = "/root/workspace/vue-app/dist"
        bucket = _bucket_name()
        client = make_minio_client()
        prefix = f"{user_id}/{target_dirname}"
        stale_key = f"{prefix}/old-only.txt"

        run_async(
            write_sandbox_files(
                user_id,
                {
                    f"{dist_root}/index.html": b"v1",
                    f"{dist_root}/old-only.txt": b"stale",
                },
            )
        )
        _invoke_deploy(
            user_id,
            path=dist_root,
            target_dirname=target_dirname,
            entry_files=["index.html"],
        )

        run_async(remove_sandbox_paths(user_id, [f"{dist_root}/old-only.txt"]))
        run_async(
            write_sandbox_files(
                user_id,
                {f"{dist_root}/index.html": b"v2"},
            )
        )
        result = _invoke_deploy(
            user_id,
            path=dist_root,
            target_dirname=target_dirname,
            entry_files=["index.html"],
        )
        try:
            assert result["total_count"] == 1
            assert httpx.get(result["urls"][0], timeout=30.0).content == b"v2"

            async def _assert_stale_gone() -> None:
                assert not await client.file_exists(
                    bucket_name=bucket, filename=stale_key
                )

            run_async(_assert_stale_gone())
        finally:
            run_async(remove_object_prefix(client, bucket, prefix))


class TestDeployToolFailures:
    def test_missing_sandbox_path_raises(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        target_dirname = f"missing-{uuid.uuid4().hex[:8]}"
        missing = "/root/workspace/no-such-project/dist"

        with pytest.raises(RuntimeError, match="沙箱内上传失败"):
            _invoke_deploy(
                user_id,
                path=missing,
                target_dirname=target_dirname,
                entry_files=["index.html"],
            )

    def test_same_user_two_deploys_use_isolated_prefixes(self):
        user_id = f"it-user-{uuid.uuid4().hex[:12]}"
        bucket = _bucket_name()
        client = make_minio_client()
        dirname_a = f"app-a-{uuid.uuid4().hex[:6]}"
        dirname_b = f"app-b-{uuid.uuid4().hex[:6]}"
        root_a = f"/root/workspace/{dirname_a}/dist"
        root_b = f"/root/workspace/{dirname_b}/dist"

        run_async(
            write_sandbox_files(
                user_id,
                {
                    f"{root_a}/index.html": b"app-a",
                    f"{root_b}/index.html": b"app-b",
                },
            )
        )
        result_a = _invoke_deploy(
            user_id,
            path=root_a,
            target_dirname=dirname_a,
            entry_files=["index.html"],
        )
        result_b = _invoke_deploy(
            user_id,
            path=root_b,
            target_dirname=dirname_b,
            entry_files=["index.html"],
        )
        try:
            assert httpx.get(result_a["urls"][0], timeout=30.0).content == b"app-a"
            assert httpx.get(result_b["urls"][0], timeout=30.0).content == b"app-b"
        finally:
            run_async(remove_object_prefix(client, bucket, f"{user_id}/{dirname_a}"))
            run_async(remove_object_prefix(client, bucket, f"{user_id}/{dirname_b}"))
