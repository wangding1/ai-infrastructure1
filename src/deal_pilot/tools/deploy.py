import base64
import json
import re
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any, TypedDict
from urllib.parse import quote

from langchain.tools import tool
from opensandbox.models.execd import RunCommandOpts
from pydantic import BaseModel, Field

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.config.nacos.nacos_config import get_config
from deal_pilot.config.nacos.nacos_schemas import OpenSandboxConfig
from deal_pilot.utils.minio import CacheStrategy, ContentTypeStrategy, MinIO
from deal_pilot.utils.sandbox import enter_sandbox
from deal_pilot.utils.shared import get_user_id

_DEPLOY_CACHE_STRATEGIES = [
    CacheStrategy(
        extensions=[
            ".html",
            ".htm",
            ".css",
            ".js",
            ".mjs",
            ".json",
            ".svg",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".ico",
            ".woff",
            ".woff2",
            ".ttf",
            ".map",
        ],
        max_age=7776000,
    ),
]

_DEPLOY_CONTENT_TYPE_STRATEGY = ContentTypeStrategy(
    extensions={
        ".html": "text/html; charset=utf-8",
        ".htm": "text/html; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".mjs": "application/javascript; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".ttf": "font/ttf",
        ".txt": "text/plain; charset=utf-8",
        ".xml": "application/xml; charset=utf-8",
        ".wasm": "application/wasm",
        ".zip": "application/zip",
        ".gz": "application/gzip",
        ".tar": "application/x-tar",
        ".pdf": "application/pdf",
    },
)

_SANDBOX_UPLOAD_SCRIPT = r"""
import asyncio
import json
import os
import sys
from pathlib import Path

from minio import CacheStrategy, ContentTypeStrategy, MinIO

_CACHE = [
    CacheStrategy(
        extensions=[
            ".html", ".htm", ".css", ".js", ".mjs", ".json", ".svg",
            ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico",
            ".woff", ".woff2", ".ttf", ".map",
        ],
        max_age=7776000,
    ),
]
_CONTENT_TYPE = ContentTypeStrategy(
    extensions={
        ".html": "text/html; charset=utf-8",
        ".htm": "text/html; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".mjs": "application/javascript; charset=utf-8",
        ".json": "application/json; charset=utf-8",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".ttf": "font/ttf",
        ".txt": "text/plain; charset=utf-8",
        ".xml": "application/xml; charset=utf-8",
        ".wasm": "application/wasm",
        ".zip": "application/zip",
        ".gz": "application/gzip",
        ".tar": "application/x-tar",
        ".pdf": "application/pdf",
    },
)


async def main() -> None:
    cfg = json.loads(os.environ["DEPLOY_PAYLOAD"])
    client = MinIO(
        endpoint=cfg["endpoint"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=cfg["secure"],
        cache_strategy=_CACHE,
        content_type_strategy=_CONTENT_TYPE,
    )
    source = Path(cfg["source_path"])
    prefix = cfg["object_prefix"]
    bucket = cfg["bucket"]
    if source.is_dir():
        result = await client.upload_dir(
            bucket_name=bucket,
            source_dir=str(source),
            prefix=prefix,
        )
        print(
            json.dumps(
                {
                    "kind": "dir",
                    "total_count": result.total_count,
                    "success_count": result.success_count,
                    "failure_count": result.failure_count,
                    "failures": result.failures,
                },
                ensure_ascii=False,
            )
        )
        return
    if source.is_file():
        object_key = f"{prefix}/{source.name}" if prefix else source.name
        try:
            await client.upload_file(
                bucket_name=bucket,
                filename=object_key,
                source_file=str(source),
            )
            payload = {
                "kind": "file",
                "total_count": 1,
                "success_count": 1,
                "failure_count": 0,
                "failures": {},
            }
        except BaseException as exc:
            payload = {
                "kind": "file",
                "total_count": 1,
                "success_count": 0,
                "failure_count": 1,
                "failures": {str(source): f"{type(exc).__name__}: {exc}"},
            }
        print(json.dumps(payload, ensure_ascii=False))
        return
    print(f"路径不存在或不是普通文件/目录: {source}", file=sys.stderr)
    sys.exit(1)


asyncio.run(main())
"""

_SANDBOX_UPLOAD_BOOTSTRAP = (
    "import base64,os; "
    "exec(base64.b64decode(os.environ['DEPLOY_UPLOAD_SCRIPT_B64']).decode())"
)


class DeployInput(BaseModel):
    path: str = Field(description="""
        沙箱内的产物路径，它可以是文件夹，也可以是文件。
        这些路径表示要最终交付给用户的产物。
        比如以下都是有效的 path 值：
        /root/workspace/my-vue-app/dist
        /root/workspace/my-vue-app/assets/charts.png
        /root/workspace/my-vue-app/other.tar.gz
        如果指定的是目录，该工具会自动递归读取该目录下的所有文件进行部署
        """)
    target_dirname: str = Field(description="""
        部署的目标目录名称。
        该工具会把 path 参数指定的产物放到云存储的某个存储路径中。
        存储路径是： <用户ID>/<target_dirname>/。
        用户 ID 由系统自动注入，你只需要指定 target_dirname 即可。
        建议该值直接取用 workspace 中的工程名字。
        """)
    entry_files: list[str] = Field(
        min_length=1,
        description="""
        指定部署完成过后，要访问的入口文件名称。
        比如：["index.html"、"static/chart.png"]
        该工具会自动拼接好完整的 URL 访问地址，最终交付给用户访问
        """,
    )


class DeployOutput(TypedDict):
    # 部署的总文件数量
    total_count: int
    # 成功部署的文件数量
    success_count: int
    # 失败的文件数量
    failure_count: int
    # 失败的原因，键为沙箱内文件的绝对路径，值是错误描述
    failures: dict[str, str]
    # 用户访问产物的入口地址集合
    urls: list[str]


def _minio_credentials() -> tuple[str, str]:
    access_key = minio_settings.access_key.strip() or "root"
    secret_key = minio_settings.secret_key.strip() or "12345678"
    return access_key, secret_key


def _resolve_endpoint_for_sandbox() -> str:
    custom = minio_settings.endpoint_in_sandbox.strip()
    if custom:
        return custom
    return "host.docker.internal:9002"


def _make_minio_client(*, endpoint: str) -> MinIO:
    access_key, secret_key = _minio_credentials()
    return MinIO(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=minio_settings.secure,
        cache_strategy=_DEPLOY_CACHE_STRATEGIES,
        content_type_strategy=_DEPLOY_CONTENT_TYPE_STRATEGY,
    )


def _normalize_target_dirname(name: str) -> str:
    normalized = name.strip().strip("/")
    if not normalized:
        raise ValueError("target_dirname 不能为空")
    if normalized in {".", ".."} or "/" in normalized or "\\" in normalized:
        raise ValueError(f"target_dirname 不合法: {name!r}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", normalized):
        raise ValueError(f"target_dirname 仅允许字母、数字、点、下划线和连字符: {name!r}")
    return normalized


def _normalize_entry_file(entry: str) -> str:
    normalized = PurePosixPath(entry.strip().lstrip("/")).as_posix()
    if not normalized or normalized in {".", ".."} or normalized.startswith("../"):
        raise ValueError(f"entry_files 项不合法: {entry!r}")
    return normalized


def _object_prefix(user_id: str, target_dirname: str) -> str:
    return f"{user_id}/{target_dirname}"


def _entry_object_key(user_id: str, target_dirname: str, entry_file: str) -> str:
    return f"{user_id}/{target_dirname}/{entry_file}"


def _build_entry_urls(
    user_id: str, target_dirname: str, entry_files: list[str]
) -> list[str]:
    bucket = minio_settings.bucket.strip() or "deal-pilot"
    custom_prefix = minio_settings.url_prefix.strip()
    if custom_prefix:
        base = custom_prefix.rstrip("/")
        return [
            f"{base}/{quote(user_id, safe='')}/{quote(target_dirname, safe='')}/"
            f"{quote(entry, safe='/')}"
            for entry in entry_files
        ]
    url_client = _make_minio_client(
        endpoint=minio_settings.endpoint.strip() or "localhost:9002"
    )
    return [
        url_client.get_file_url(
            bucket_name=bucket,
            filename=_entry_object_key(user_id, target_dirname, entry),
        )
        for entry in entry_files
    ]


def _map_dir_failures(source_dir: str, failures: dict[str, str]) -> dict[str, str]:
    root = Path(source_dir)
    mapped: dict[str, str] = {}
    for relative, message in failures.items():
        mapped[str((root / relative).resolve())] = message
    return mapped


def _stdout_text(execution: Any) -> str:
    parts: list[str] = []
    for message in execution.logs.stdout:
        parts.append(message.text)
    return "".join(parts).strip()


def _stderr_text(execution: Any) -> str:
    parts: list[str] = []
    for message in execution.logs.stderr:
        parts.append(message.text)
    return "".join(parts).strip()


async def _run_sandbox_upload(source_path: str, object_prefix: str) -> dict[str, Any]:
    access_key, secret_key = _minio_credentials()
    payload = json.dumps(
        {
            "source_path": source_path,
            "object_prefix": object_prefix,
            "endpoint": _resolve_endpoint_for_sandbox(),
            "access_key": access_key,
            "secret_key": secret_key,
            "secure": minio_settings.secure,
            "bucket": minio_settings.bucket.strip() or "deal-pilot",
        },
        ensure_ascii=False,
    )
    sandbox_config = await get_config("sandbox_config", OpenSandboxConfig)
    upload_timeout_seconds = max(sandbox_config.command_timeout_seconds, 600)
    command = (
        "cd /sandbox_utils && uv run python -c "
        + json.dumps(_SANDBOX_UPLOAD_BOOTSTRAP)
    )
    script_b64 = base64.b64encode(_SANDBOX_UPLOAD_SCRIPT.encode()).decode()
    async with enter_sandbox(get_user_id()) as sandbox:
        execution = await sandbox.commands.run(
            command,
            opts=RunCommandOpts(
                timeout=timedelta(seconds=upload_timeout_seconds),
                working_directory="/sandbox_utils",
                envs={
                    "DEPLOY_PAYLOAD": payload,
                    "DEPLOY_UPLOAD_SCRIPT_B64": script_b64,
                },
            ),
        )
    if execution.exit_code != 0:
        detail = _stderr_text(execution) or _stdout_text(execution) or "未知错误"
        raise RuntimeError(f"沙箱内上传失败（退出码 {execution.exit_code}）：{detail}")
    stdout = _stdout_text(execution)
    if not stdout:
        raise RuntimeError("沙箱内上传未返回结果")
    return json.loads(stdout)


async def _ensure_bucket() -> None:
    client = _make_minio_client(endpoint=minio_settings.endpoint.strip() or "localhost:9002")
    bucket = minio_settings.bucket.strip() or "deal-pilot"
    await client.create_bucket(bucket, allow_public_read=True)


async def _clear_deploy_prefix(prefix: str) -> None:
    """删除该部署前缀下已有对象，避免目录二次部署残留旧文件。"""
    client = _make_minio_client(endpoint=minio_settings.endpoint.strip() or "localhost:9002")
    bucket = minio_settings.bucket.strip() or "deal-pilot"
    normalized = prefix.strip("/")
    if not normalized:
        return
    for key in await client.list_files(bucket, prefix=f"{normalized}/"):
        await client.remove_file(bucket_name=bucket, filename=key)
    if await client.file_exists(bucket_name=bucket, filename=normalized):
        await client.remove_file(bucket_name=bucket, filename=normalized)


@tool(args_schema=DeployInput)
async def deploy(
    path: str, target_dirname: str, entry_files: list[str]
) -> DeployOutput:
    """
    将沙箱内的文件或目录部署到云存储，返回部署统计、失败原因和入口访问地址。

    参数：
        path: 沙箱内产物的路径；目录会递归部署其中的所有文件。
        target_dirname: 云存储中的目标目录名称，存储路径为 <用户ID>/<target_dirname>/。
            每次部署会先清空该前缀下已有对象，再上传本次产物。
        entry_files: 要交付给用户的入口文件路径，相对于部署后的目标目录。

    返回：
        DeployOutput 字典，包含以下字段：
        total_count: 本次部署的总文件数量。
        success_count: 成功部署的文件数量。
        failure_count: 部署失败的文件数量；为 0 表示所有文件部署成功。
        failures: 失败文件及原因，键为沙箱内文件的绝对路径，值为错误描述。
        urls: 根据 entry_files 拼接的入口访问地址列表。

    调用示例（目录部署，全部成功）：

    deploy(
        "/root/workspace/vue-app/dist",
        "vue-app",
        ["index.html", "static/chart.png"]
    )

    得到结果示例：
    {
        "total_count": 5,
        "success_count": 5,
        "failure_count": 0,
        "failures": {},
        "urls": [
            "http://deploy.com/u_123/vue-app/index.html",
            "http://deploy.com/u_123/vue-app/static/chart.png"
        ]
    }

    调用示例（单文件部署，全部成功）：

    deploy(
        "/root/workspace/deep-research/dist/reporter.zip",
        "deep-research",
        ["reporter.zip"]
    )

    得到结果示例：

    {
        "total_count": 1,
        "success_count": 1,
        "failure_count": 0,
        "failures": {},
        "urls": [
            "http://deploy.com/u_123/deep-research/reporter.zip"
        ]
    }
    """
    source_path = path.strip()
    if not source_path:
        raise ValueError("path 不能为空")

    dirname = _normalize_target_dirname(target_dirname)
    entries = [_normalize_entry_file(item) for item in entry_files]
    user_id = get_user_id()
    prefix = _object_prefix(user_id, dirname)

    await _ensure_bucket()
    await _clear_deploy_prefix(prefix)
    upload_result = await _run_sandbox_upload(source_path, prefix)

    failures: dict[str, str] = dict(upload_result.get("failures") or {})
    if upload_result.get("kind") == "dir":
        failures = _map_dir_failures(source_path, failures)

    urls = _build_entry_urls(user_id, dirname, entries)

    return DeployOutput(
        total_count=int(upload_result["total_count"]),
        success_count=int(upload_result["success_count"]),
        failure_count=int(upload_result["failure_count"]),
        failures=failures,
        urls=urls,
    )


deploy.handle_tool_error = True
