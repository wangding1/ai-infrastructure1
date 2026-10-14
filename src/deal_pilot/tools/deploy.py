import base64
import json
import re
import shlex
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Literal, TypedDict

from langchain.tools import tool
from langgraph.runtime import get_runtime
from langgraph.store.base import BaseStore
from opensandbox.models.execd import RunCommandOpts
from pydantic import BaseModel, Field

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.config.nacos.nacos_config import get_config
from deal_pilot.config.nacos.nacos_schemas import OpenSandboxConfig
from deal_pilot.utils.minio import CacheStrategy, ContentTypeStrategy, MinIO
from deal_pilot.utils.sandbox import enter_sandbox
from deal_pilot.utils.shared import get_user_id

_BUCKET_NAME = "deal-pilot"

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
from pathlib import Path, PurePosixPath

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


def _validate_entries(source: Path, entry_files: list[str]) -> None:
    if source.is_dir():
        has_file = False
        for dirpath, _, filenames in os.walk(source, followlinks=False):
            current = Path(dirpath)
            for name in filenames:
                local_path = current / name
                if local_path.is_symlink() or not local_path.is_file():
                    continue
                has_file = True
                break
            if has_file:
                break
        if not has_file:
            print("部署目录为空或无可上传的普通文件", file=sys.stderr)
            sys.exit(1)
        for entry in entry_files:
            entry_path = source / entry
            if not entry_path.is_file():
                print(f"entry_files 中的入口不是待上传的普通文件: {entry}", file=sys.stderr)
                sys.exit(1)
        return
    if source.is_file():
        if len(entry_files) != 1:
            print("单文件部署时 entry_files 必须只包含一个入口", file=sys.stderr)
            sys.exit(1)
        if PurePosixPath(entry_files[0]).name != source.name:
            print(
                f"单文件部署时入口文件名必须与源文件 basename 一致: {entry_files[0]!r}",
                file=sys.stderr,
            )
            sys.exit(1)
        return
    print(f"路径不存在或不是普通文件/目录: {source}", file=sys.stderr)
    sys.exit(1)


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
    entry_files = cfg.get("entry_files") or []
    _validate_entries(source, entry_files)
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

        注意：访问 URL 落在子路径下（非站点根 /）。Vite 等前端默认 base 为 /，
        构建产物中资源引用常为绝对路径（如 /assets/index.js），部署到
        <用户ID>/<target_dirname>/ 后浏览器仍从根路径拉资源，会导致 404。
        部署 SPA 前请将 vite.config 的 base 设为与发布子路径一致（如 ./ 或
        /<用户ID>/<target_dirname>/），勿仅上传 dist 而不检查 base。
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
    total_count: int
    success_count: int
    failure_count: int
    failures: dict[str, str]
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


def _host_minio_client() -> MinIO:
    return _make_minio_client(
        endpoint=minio_settings.endpoint.strip() or "localhost:9002"
    )


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
    url_client = _host_minio_client()
    return [
        url_client.get_file_url(
            bucket_name=_BUCKET_NAME,
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


def _sandbox_store() -> BaseStore | None:
    try:
        runtime = get_runtime()
        return runtime.store
    except (RuntimeError, AttributeError):
        return None


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


async def _sandbox_source_kind(
    source_path: str, store: BaseStore | None
) -> Literal["file", "dir", "missing"]:
    quoted = shlex.quote(source_path)
    command = (
        f"if [ -d {quoted} ]; then echo dir; "
        f"elif [ -f {quoted} ]; then echo file; "
        f"else echo missing; fi"
    )
    async with enter_sandbox(get_user_id(), store=store) as sandbox:
        execution = await sandbox.commands.run(command)
    text = _stdout_text(execution).strip()
    if text in {"file", "dir", "missing"}:
        return text
    raise RuntimeError(f"无法判断沙箱路径类型: {source_path}")


async def _run_sandbox_upload(
    source_path: str,
    object_prefix: str,
    entry_files: list[str],
    store: BaseStore | None,
) -> dict[str, Any]:
    access_key, secret_key = _minio_credentials()
    payload = json.dumps(
        {
            "source_path": source_path,
            "object_prefix": object_prefix,
            "endpoint": _resolve_endpoint_for_sandbox(),
            "access_key": access_key,
            "secret_key": secret_key,
            "secure": minio_settings.secure,
            "bucket": _BUCKET_NAME,
            "entry_files": entry_files,
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
    async with enter_sandbox(get_user_id(), store=store) as sandbox:
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
        if "entry_files" in detail or "部署目录为空" in detail or "单文件部署" in detail:
            raise ValueError(detail)
        raise RuntimeError(f"沙箱内上传失败（退出码 {execution.exit_code}）：{detail}")
    stdout = _stdout_text(execution)
    if not stdout:
        raise RuntimeError("沙箱内上传未返回结果")
    return json.loads(stdout)


async def _ensure_bucket() -> None:
    client = _host_minio_client()
    await client.create_bucket(_BUCKET_NAME, allow_public_read=True)


async def _clear_deploy_prefix(prefix: str) -> None:
    client = _host_minio_client()
    normalized = prefix.strip("/")
    if not normalized:
        return
    for key in await client.list_files(_BUCKET_NAME, prefix=f"{normalized}/"):
        await client.remove_file(bucket_name=_BUCKET_NAME, filename=key)
    if await client.file_exists(bucket_name=_BUCKET_NAME, filename=normalized):
        await client.remove_file(bucket_name=_BUCKET_NAME, filename=normalized)


def _failed_entry_paths(
    source_path: str,
    kind: str,
    failures: dict[str, str],
) -> set[str]:
    if kind == "file":
        return set(failures.keys())
    root = Path(source_path)
    return {str((root / rel).resolve()) for rel in failures}


async def _successful_entries(
    *,
    user_id: str,
    target_dirname: str,
    source_path: str,
    kind: str,
    entries: list[str],
    failures: dict[str, str],
) -> list[str]:
    client = _host_minio_client()
    failed_paths = _failed_entry_paths(source_path, kind, failures)
    successful: list[str] = []
    root = Path(source_path)
    for entry in entries:
        if kind == "dir":
            candidate = str((root / entry).resolve())
            if candidate in failed_paths:
                continue
        elif kind == "file" and source_path in failed_paths:
            continue
        object_key = _entry_object_key(user_id, target_dirname, entry)
        if await client.file_exists(bucket_name=_BUCKET_NAME, filename=object_key):
            successful.append(entry)
    return successful


@tool(args_schema=DeployInput)
async def deploy(
    path: str, target_dirname: str, entry_files: list[str]
) -> DeployOutput:
    """
    将沙箱内的文件或目录部署到云存储，返回部署统计、失败原因和入口访问地址。

    目录部署会先清空 <用户ID>/<target_dirname>/ 下已有对象再上传；
    单文件部署仅覆盖同名对象，保留同前缀下的其他对象。

    前端静态站点：产物发布在子路径 <用户ID>/<target_dirname>/ 下。若构建工具
    （如 Vite，默认 base=/）在 HTML/JS 中使用以 / 开头的资源绝对路径，部署后
    入口页可打开但静态资源易 404；部署前应调整 base 或确认资源为相对路径。
    """
    source_path = path.strip()
    if not source_path:
        raise ValueError("path 不能为空")

    dirname = _normalize_target_dirname(target_dirname)
    entries = [_normalize_entry_file(item) for item in entry_files]
    user_id = get_user_id()
    prefix = _object_prefix(user_id, dirname)
    store = _sandbox_store()

    kind = await _sandbox_source_kind(source_path, store)
    if kind == "missing":
        raise ValueError(f"沙箱内路径不存在: {source_path}")

    await _ensure_bucket()
    if kind == "dir":
        await _clear_deploy_prefix(prefix)

    upload_result = await _run_sandbox_upload(
        source_path, prefix, entries, store
    )

    failures: dict[str, str] = dict(upload_result.get("failures") or {})
    upload_kind = str(upload_result.get("kind", kind))
    if upload_kind == "dir":
        failures = _map_dir_failures(source_path, failures)

    successful_entries = await _successful_entries(
        user_id=user_id,
        target_dirname=dirname,
        source_path=source_path,
        kind=upload_kind,
        entries=entries,
        failures=failures,
    )
    urls = _build_entry_urls(user_id, dirname, successful_entries)

    return DeployOutput(
        total_count=int(upload_result["total_count"]),
        success_count=int(upload_result["success_count"]),
        failure_count=int(upload_result["failure_count"]),
        failures=failures,
        urls=urls,
    )


deploy.handle_tool_error = True
