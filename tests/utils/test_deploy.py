"""deploy 工具单元测试（参数校验、URL 拼接、失败路径映射，不依赖沙箱）。"""

import pytest
from pydantic import ValidationError

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.tools.deploy import (
    _build_entry_urls,
    _map_dir_failures,
    _normalize_entry_file,
    _normalize_target_dirname,
    _object_prefix,
    deploy,
)
from support import langgraph_user_context, run_async


class TestNormalizeTargetDirname:
    def test_accepts_doc_style_names(self):
        assert _normalize_target_dirname("vue-app") == "vue-app"
        assert _normalize_target_dirname("deep-research") == "deep-research"
        assert _normalize_target_dirname(" my.app_1 ") == "my.app_1"

    def test_strips_surrounding_slashes(self):
        assert _normalize_target_dirname("/vue-app/") == "vue-app"

    @pytest.mark.parametrize(
        "raw",
        ["", "  ", ".", "..", "a/b", r"a\b", "-bad", "中文"],
    )
    def test_rejects_invalid(self, raw: str):
        with pytest.raises(ValueError):
            _normalize_target_dirname(raw)


class TestNormalizeEntryFile:
    def test_strips_leading_slashes_and_whitespace(self):
        assert _normalize_entry_file("index.html") == "index.html"
        assert _normalize_entry_file("/static/chart.png") == "static/chart.png"
        assert _normalize_entry_file("  /assets/app.js  ") == "assets/app.js"

    @pytest.mark.parametrize(
        "raw",
        ["", ".", "..", "../secret", "/../x"],
    )
    def test_rejects_invalid(self, raw: str):
        with pytest.raises(ValueError):
            _normalize_entry_file(raw)


class TestMapDirFailures:
    def test_maps_relative_keys_to_absolute_sandbox_paths(self):
        source = "/root/workspace/vue-app/dist"
        mapped = _map_dir_failures(
            source,
            {"static/chart.png": "boom", "index.html": "denied"},
        )
        assert mapped == {
            "/root/workspace/vue-app/dist/static/chart.png": "boom",
            "/root/workspace/vue-app/dist/index.html": "denied",
        }


class TestBuildEntryUrls:
    def test_uses_minio_endpoint_when_url_prefix_empty(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(minio_settings, "url_prefix", "")
        monkeypatch.setattr(minio_settings, "endpoint", "localhost:9002")
        monkeypatch.setattr(minio_settings, "bucket", "deal-pilot")
        monkeypatch.setattr(minio_settings, "secure", False)
        urls = _build_entry_urls(
            "u_123",
            "vue-app",
            ["index.html", "static/chart.png"],
        )
        assert urls == [
            "http://localhost:9002/deal-pilot/u_123/vue-app/index.html",
            "http://localhost:9002/deal-pilot/u_123/vue-app/static/chart.png",
        ]

    def test_uses_custom_url_prefix_from_settings(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            minio_settings,
            "url_prefix",
            "http://deploy.com/auto-prefix",
        )
        urls = _build_entry_urls(
            "u_123",
            "vue-app",
            ["index.html", "static/chart.png"],
        )
        assert urls == [
            "http://deploy.com/auto-prefix/u_123/vue-app/index.html",
            "http://deploy.com/auto-prefix/u_123/vue-app/static/chart.png",
        ]


class TestDeployInputValidation:
    def test_rejects_empty_path_before_sandbox(self):
        async def _run() -> None:
            with langgraph_user_context("u-it"):
                await deploy.ainvoke(
                    {
                        "path": "   ",
                        "target_dirname": "vue-app",
                        "entry_files": ["index.html"],
                    }
                )

        with pytest.raises(ValueError, match="path 不能为空"):
            run_async(_run())

    def test_rejects_invalid_target_dirname(self):
        async def _run() -> None:
            with langgraph_user_context("u-it"):
                await deploy.ainvoke(
                    {
                        "path": "/root/workspace/vue-app/dist",
                        "target_dirname": "../evil",
                        "entry_files": ["index.html"],
                    }
                )

        with pytest.raises(ValueError, match="target_dirname"):
            run_async(_run())

    def test_rejects_invalid_entry_file(self):
        async def _run() -> None:
            with langgraph_user_context("u-it"):
                await deploy.ainvoke(
                    {
                        "path": "/root/workspace/vue-app/dist",
                        "target_dirname": "vue-app",
                        "entry_files": ["../etc/passwd"],
                    }
                )

        with pytest.raises(ValueError, match="entry_files"):
            run_async(_run())

    def test_requires_langgraph_user_id(self):
        async def _run() -> None:
            await deploy.ainvoke(
                {
                    "path": "/root/workspace/vue-app/dist",
                    "target_dirname": "vue-app",
                    "entry_files": ["index.html"],
                }
            )

        with pytest.raises(RuntimeError, match="langgraph_auth_user_id"):
            run_async(_run())

    def test_pydantic_requires_at_least_one_entry_file(self):
        with pytest.raises(ValidationError):
            run_async(
                deploy.ainvoke(
                    {
                        "path": "/root/x",
                        "target_dirname": "vue-app",
                        "entry_files": [],
                    }
                )
            )


class TestObjectPrefix:
    def test_matches_doc_layout(self):
        assert _object_prefix("u_123", "vue-app") == "u_123/vue-app"
