"""MinIO 异步客户端，基于 aiobotocore 实现 S3 兼容 API。"""

import asyncio
import json
import os
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, cast
from urllib.parse import quote

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

# 默认缓存时间为 90 天，单位为秒。
_DEFAULT_MAX_AGE = 7776000
_MIN_PRESIGN_SECONDS = 1
_MAX_PRESIGN_SECONDS = 7 * 24 * 60 * 60
_PUBLIC_READ_ACTIONS = frozenset({"s3:GetObject", "s3:GetObject*"})


@dataclass
class CacheStrategy:
    """按文件后缀配置上传对象的 Cache-Control。

    extensions: 后缀列表，例如 [".png", ".jpg"]；匹配时忽略大小写。
        空列表不匹配任何文件。后缀必须以点开头且非空。
    max_age: 缓存秒数，必须大于等于 0；0 表示 no-store，
        大于 0 表示 max-age=<秒数>。默认值为 90 天。
    """

    extensions: list[str]
    max_age: int = _DEFAULT_MAX_AGE


@dataclass
class ContentTypeStrategy:
    """配置上传对象的默认 Content-Type 和文件后缀映射。

    default_content_type: 未匹配后缀或没有后缀时使用的非空 Content-Type，
        默认为 application/octet-stream。
    extensions: 后缀到非空 Content-Type 的映射，例如
        {".html": "text/html; charset=utf-8", ".png": "image/png"}。
        后缀必须以点开头且非空；匹配时忽略大小写，
        忽略大小写后重复的后缀视为不合法配置。

    write_file、upload_file 和 upload_dir 均按目标对象键最后一段的最后一个后缀匹配，
    例如 archive.tar.gz 匹配 .gz；映射优先于默认值。
    不根据本地文件名、文件内容或系统 MIME 配置自动推断类型。
    """

    default_content_type: str = "application/octet-stream"
    extensions: dict[str, str] = field(default_factory=dict)


@dataclass
class UploadDirResult:
    """目录上传完成后的汇总结果。

    total_count: 本次需要上传的普通文件总数，不包含跳过的文件或目录。
    success_count: 上传成功的文件数量。
    failure_count: 上传失败的文件数量。
    failures: 每个失败文件的原因；键为相对于 source_dir 的本地路径，
        分隔符统一为 /，不包含目标 prefix；值为包含异常类型和错误消息的字符串。

    total_count = success_count + failure_count；failure_count = len(failures)。
    无可上传文件时三个数量均为 0，failures 为 {}。
    """

    total_count: int
    success_count: int
    failure_count: int
    failures: dict[str, str]


def _validate_extension(ext: str, *, field_name: str) -> str:
    if not ext or not ext.startswith(".") or ext == ".":
        raise ValueError(f"{field_name} 后缀必须以点开头且非空: {ext!r}")
    return ext


def _validate_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip()
    if not endpoint:
        raise ValueError("endpoint 不能为空")
    if "://" in endpoint:
        raise ValueError("endpoint 不能包含协议")
    if "/" in endpoint or "?" in endpoint or "#" in endpoint:
        raise ValueError("endpoint 不能包含路径或查询参数")
    return endpoint


def _validate_cache_strategies(strategies: list[CacheStrategy] | None) -> list[CacheStrategy]:
    if not strategies:
        return []
    for strategy in strategies:
        if strategy.max_age < 0:
            raise ValueError("cache_strategy.max_age 必须大于等于 0")
        for ext in strategy.extensions:
            _validate_extension(ext, field_name="cache_strategy.extensions")
    return list(strategies)


def _validate_content_type_strategy(
    strategy: ContentTypeStrategy | None,
) -> ContentTypeStrategy:
    if strategy is None:
        return ContentTypeStrategy()
    default = strategy.default_content_type.strip()
    if not default:
        raise ValueError("content_type_strategy.default_content_type 不能为空")
    seen: set[str] = set()
    normalized: dict[str, str] = {}
    for ext, content_type in strategy.extensions.items():
        _validate_extension(ext, field_name="content_type_strategy.extensions")
        ct = content_type.strip()
        if not ct:
            raise ValueError("content_type_strategy.extensions 的 Content-Type 不能为空")
        key = ext.lower()
        if key in seen:
            raise ValueError(f"content_type_strategy.extensions 后缀重复: {ext!r}")
        seen.add(key)
        normalized[key] = ct
    return ContentTypeStrategy(
        default_content_type=default,
        extensions=normalized,
    )


def _object_suffix(object_key: str) -> str | None:
    segment = object_key.rsplit("/", 1)[-1]
    dot = segment.rfind(".")
    if dot <= 0:
        return None
    return segment[dot:]


def _normalize_object_key(filename: str) -> str:
    key = filename.lstrip("/")
    if not key:
        raise ValueError("filename 不能为空")
    return key


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")


def _join_object_key(prefix: str, relative_path: str) -> str:
    rel = relative_path.replace(os.sep, "/")
    if not prefix:
        return rel
    return f"{prefix}/{rel}"


def _expires_seconds(expires: timedelta) -> int:
    total = expires.total_seconds()
    if total != int(total):
        raise ValueError("expires 必须是整数秒")
    seconds = int(total)
    if seconds < _MIN_PRESIGN_SECONDS or seconds > _MAX_PRESIGN_SECONDS:
        raise ValueError("expires 必须在 1 秒至 7 天之间")
    return seconds


def _is_not_found_error(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    return code in ("404", "NoSuchKey", "NotFound", "NoSuchBucket")


def _failure_message(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _public_read_resource(bucket_name: str) -> str:
    return f"arn:aws:s3:::{bucket_name}/*"


def _statement_allows_public_read(statement: object, bucket_name: str) -> bool:
    if not isinstance(statement, dict):
        return False
    if statement.get("Effect") != "Allow":
        return False
    principal = statement.get("Principal")
    if principal != "*" and principal != {"AWS": "*"}:
        return False
    actions = statement.get("Action")
    if isinstance(actions, str):
        actions = [actions]
    if not isinstance(actions, list):
        return False
    if not any(action in _PUBLIC_READ_ACTIONS or action == "*" for action in actions):
        return False
    resources = statement.get("Resource")
    if isinstance(resources, str):
        resources = [resources]
    if not isinstance(resources, list):
        return False
    target = _public_read_resource(bucket_name)
    return any(resource in (target, "arn:aws:s3:::*", "*") for resource in resources)


def _merge_public_read_policy(existing_policy: str | None, bucket_name: str) -> str:
    document: dict[str, object]
    if existing_policy:
        document = json.loads(existing_policy)
    else:
        document = {"Version": "2012-10-17", "Statement": []}

    statements = document.get("Statement")
    if not isinstance(statements, list):
        statements = []
        document["Statement"] = statements

    if any(_statement_allows_public_read(item, bucket_name) for item in statements):
        return json.dumps(document)

    statements.append(
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [_public_read_resource(bucket_name)],
        }
    )
    return json.dumps(document)


class MinIO:
    """基于 aiobotocore 的 MinIO 异步操作客户端。

    通用约定：
        - bucket_name 遵循服务端桶命名规则。
        - filename 是非空对象键，例如 "a/b/c/1.html"，不包含桶名。
          开头的 / 会被移除；不解析 .. 或合并中间的 /。
          去除开头的 / 后仍为空时，抛出 ValueError。
        - 除 create_bucket 外，方法不自动创建桶或修改访问权限。
        - 参数不合法抛出 ValueError；本地文件错误保留对应 OSError。
          服务端和网络错误原样向调用方传播，不转换为成功或空结果；
          upload_dir 的单文件错误例外，记录到汇总结果中。
    """

    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
        cache_strategy: list[CacheStrategy] | None = None,
        content_type_strategy: ContentTypeStrategy | None = None,
    ) -> None:
        """声明客户端配置；构造阶段不连接服务端。

        Args:
            endpoint: API 地址，例如 localhost:9002；不含协议、路径或查询参数。
            access_key: 非空访问密钥。
            secret_key: 非空秘密密钥。
            secure: True 使用 HTTPS，False 使用 HTTP。
            cache_strategy: None 或空列表表示所有上传文件均 no-store。
                未匹配的文件也使用 no-store；多个策略匹配时后者覆盖前者。
                策略仅影响此客户端后续上传的对象，不修改已存在对象。
            content_type_strategy: 上传时使用的 Content-Type 策略。
                None 等同于默认策略，即所有文件均使用 application/octet-stream。
                策略仅影响后续上传的对象，不修改已存在对象。

        Raises:
            ValueError: 地址、密钥、缓存策略或 Content-Type 策略不合法。
        """
        self._endpoint = _validate_endpoint(endpoint)
        access_key = access_key.strip()
        secret_key = secret_key.strip()
        if not access_key:
            raise ValueError("access_key 不能为空")
        if not secret_key:
            raise ValueError("secret_key 不能为空")
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure
        self._cache_strategy = _validate_cache_strategies(cache_strategy)
        self._content_type_strategy = _validate_content_type_strategy(
            content_type_strategy
        )
        scheme = "https" if secure else "http"
        self._endpoint_url = f"{scheme}://{self._endpoint}"
        self._session = get_session()
        self._client_config: Any = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        )

    @asynccontextmanager
    async def _client(self) -> AsyncIterator["S3Client"]:
        async with self._session.create_client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name="us-east-1",
            config=self._client_config,
        ) as client:
            yield cast("S3Client", client)

    def _cache_control_for(self, object_key: str) -> str:
        suffix = _object_suffix(object_key)
        if suffix is None:
            return "no-store"
        normalized = suffix.lower()
        control = "no-store"
        for strategy in self._cache_strategy:
            for ext in strategy.extensions:
                if ext.lower() == normalized:
                    control = (
                        "no-store"
                        if strategy.max_age == 0
                        else f"max-age={strategy.max_age}"
                    )
        return control

    def _content_type_for(self, object_key: str) -> str:
        suffix = _object_suffix(object_key)
        if suffix is None:
            return self._content_type_strategy.default_content_type
        mapped = self._content_type_strategy.extensions.get(suffix.lower())
        return (
            mapped
            if mapped is not None
            else self._content_type_strategy.default_content_type
        )

    async def _apply_public_read_policy(
        self, client: "S3Client", bucket_name: str
    ) -> None:
        existing: str | None = None
        try:
            response = await client.get_bucket_policy(Bucket=bucket_name)
            existing = response.get("Policy")
        except ClientError as exc:
            if not _is_not_found_error(exc):
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in ("NoSuchBucketPolicy", "PolicyNotFound"):
                    raise
        policy = _merge_public_read_policy(existing, bucket_name)
        await client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

    async def _empty_bucket(self, client: "S3Client", bucket_name: str) -> None:
        paginator = client.get_paginator("list_object_versions")
        async for page in paginator.paginate(Bucket=bucket_name):
            delete_entries: list[dict[str, str]] = []
            for version in page.get("Versions") or []:
                key = version.get("Key")
                version_id = version.get("VersionId")
                if key is None or version_id is None:
                    continue
                delete_entries.append({"Key": key, "VersionId": version_id})
            for marker in page.get("DeleteMarkers") or []:
                key = marker.get("Key")
                version_id = marker.get("VersionId")
                if key is None or version_id is None:
                    continue
                delete_entries.append({"Key": key, "VersionId": version_id})
            if delete_entries:
                await client.delete_objects(
                    Bucket=bucket_name,
                    Delete=cast(
                        Any,
                        {"Objects": delete_entries, "Quiet": True},
                    ),
                )

        list_paginator = client.get_paginator("list_objects_v2")
        async for page in list_paginator.paginate(Bucket=bucket_name):
            objects = page.get("Contents") or []
            delete_keys = [
                {"Key": key}
                for item in objects
                for key in [item.get("Key")]
                if key is not None
            ]
            if not delete_keys:
                continue
            await client.delete_objects(
                Bucket=bucket_name,
                Delete=cast(Any, {"Objects": delete_keys, "Quiet": True}),
            )

        upload_paginator = client.get_paginator("list_multipart_uploads")
        async for page in upload_paginator.paginate(Bucket=bucket_name):
            for upload in page.get("Uploads") or []:
                key = upload.get("Key")
                upload_id = upload.get("UploadId")
                if key is None or upload_id is None:
                    continue
                await client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                )

    async def create_bucket(
        self, bucket_name: str, allow_public_read: bool = True
    ) -> None:
        """确保桶存在，并应用公共读约定。

        Args:
            bucket_name: 桶名；已存在时跳过建桶，仍应用公共读约定。
            allow_public_read: True 时允许任何人匿名读取桶内所有对象，
                不授予匿名写入、删除或列举权限，并保留无关的策略语句。
                False 时不新增公共读权限，也不撤销已有策略；不保证桶私有。

        建桶和设置策略不是原子操作；策略设置失败时桶可能已经创建，
        错误向调用方传播。重复调用不重复添加同一公共读策略。
        """
        async with self._client() as client:
            try:
                await client.head_bucket(Bucket=bucket_name)
            except ClientError as exc:
                if _is_not_found_error(exc):
                    await client.create_bucket(Bucket=bucket_name)
                else:
                    raise
            if allow_public_read:
                await self._apply_public_read_policy(client, bucket_name)

    async def bucket_exists(self, bucket_name: str) -> bool:
        """桶不存在时返回 False，存在时返回 True；权限和网络错误仍抛出。"""
        async with self._client() as client:
            try:
                await client.head_bucket(Bucket=bucket_name)
            except ClientError as exc:
                if _is_not_found_error(exc):
                    return False
                raise
            return True

    async def remove_bucket(self, bucket_name: str, force: bool = False) -> None:
        """删除桶；桶不存在时忽略。

        Args:
            bucket_name: 要删除的桶名。
            force: False 时仅删除空桶，非空时抛出服务端错误。
                True 时先删除桶内全部对象、历史版本和删除标记，再删除桶，
                无论桶是否为空。已删除的数据不可恢复。

        强制删除不是原子操作，失败时可能已经删除部分数据，不回滚。
        权限不足、对象保留限制或并发写入等导致删除失败时，向调用方抛出错误；
        force 不绕过服务端权限或保留规则，也不将失败视为成功。
        """
        async with self._client() as client:
            try:
                await client.head_bucket(Bucket=bucket_name)
            except ClientError as exc:
                if _is_not_found_error(exc):
                    return
                raise
            if force:
                await self._empty_bucket(client, bucket_name)
            await client.delete_bucket(Bucket=bucket_name)

    async def write_file(
        self,
        *,
        bucket_name: str,
        filename: str,
        stream: BinaryIO,
    ) -> None:
        """从二进制流当前位置读取到结尾，上传为指定对象。

        同名对象覆盖当前内容；开启版本控制时由服务端生成新版本。
        应用匹配的缓存策略。空流允许上传为零字节对象。
        Content-Type 由构造器中的 content_type_strategy 按 filename 决定。
        调用方负责关闭流；不回到流起点、不关闭流，失败后不保证流位置。
        文本应先编码为字节。
        """
        object_key = _normalize_object_key(filename)
        body = stream.read()
        async with self._client() as client:
            await client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=body,
                ContentType=self._content_type_for(object_key),
                CacheControl=self._cache_control_for(object_key),
            )

    async def upload_file(
        self,
        *,
        bucket_name: str,
        filename: str,
        source_file: str,
    ) -> None:
        """上传单个本地文件；覆盖和缓存约定与 write_file 相同。

        source_file 为本地路径，必须指向可读的普通文件。
        Content-Type 由构造器中的 content_type_strategy 按目标 filename 决定。
        本方法负责打开并关闭本地文件，不修改本地内容。
        """
        object_key = _normalize_object_key(filename)
        path = Path(source_file)
        if not path.is_file():
            if not path.exists():
                raise FileNotFoundError(source_file)
            raise IsADirectoryError(f"不是普通文件: {source_file}")
        with path.open("rb") as stream:
            body = stream.read()
        async with self._client() as client:
            await client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=body,
                ContentType=self._content_type_for(object_key),
                CacheControl=self._cache_control_for(object_key),
            )

    async def upload_dir(
        self,
        *,
        bucket_name: str,
        source_dir: str,
        prefix: str = "",
        max_concurrency: int = 10,
    ) -> UploadDirResult:
        """递归上传本地目录中的普通文件，保留相对目录结构。

        Args:
            bucket_name: 已存在的目标桶名。
            source_dir: 本地目录路径；不存在时抛出 FileNotFoundError，
                不是目录时抛出 NotADirectoryError。
            prefix: 目标对象键前缀，默认为空；移除首尾的 / 后，
                非空时通过一个 / 与文件相对路径连接。
            max_concurrency: 同时进行的文件上传数量上限，默认为 10，
                必须为正整数，否则抛出 ValueError。

        例如 source_dir 下的 images/logo.png，在 prefix="assets" 时
        上传为 assets/images/logo.png；不包含 source_dir 自身的目录名。
        相对路径的分隔符统一为 /。包含隐藏文件，跳过空目录和符号链接；
        其他非普通文件也跳过，无可上传文件时返回空汇总结果。
        每个文件的覆盖、缓存和 Content-Type 约定与 upload_file 相同，
        均按最终对象键匹配策略。不删除桶内未包含在本地目录中的对象。
        不修改本地内容；负责关闭本次上传打开的文件。
        上传前完成目录扫描，确定本次需要上传的文件集合。
        参数校验或目录扫描失败时直接抛出异常，不开始上传。
        并发上传时，单个文件的读取、服务端或网络错误均记录到 failures，
        不影响其他文件继续上传；等待全部文件处理完成后返回汇总结果。
        例如 images/logo.png 上传失败时，failures 的键为 "images/logo.png"，
        即使 prefix="assets"，也不包含 "assets/"。
        调用方取消任务时仍传播取消，不作为单文件失败处理。
        已完成的上传不回滚，整个目录上传不是原子操作。

        Returns:
            UploadDirResult：总数量、成功数量、失败数量及逐文件失败原因。
        """
        if max_concurrency <= 0:
            raise ValueError("max_concurrency 必须为正整数")
        root = Path(source_dir)
        if not root.exists():
            raise FileNotFoundError(source_dir)
        if not root.is_dir():
            raise NotADirectoryError(source_dir)

        normalized_prefix = _normalize_prefix(prefix)
        files: list[tuple[str, Path]] = []
        for dirpath, _, filenames in os.walk(root, followlinks=False):
            current = Path(dirpath)
            for name in filenames:
                local_path = current / name
                if local_path.is_symlink() or not local_path.is_file():
                    continue
                relative = local_path.relative_to(root).as_posix()
                files.append((relative, local_path))

        total = len(files)
        if total == 0:
            return UploadDirResult(0, 0, 0, {})

        semaphore = asyncio.Semaphore(max_concurrency)
        failures: dict[str, str] = {}
        success_count = 0
        lock = asyncio.Lock()

        async def upload_one(relative_path: str, local_path: Path) -> None:
            nonlocal success_count
            async with semaphore:
                object_key = _join_object_key(normalized_prefix, relative_path)
                try:
                    with local_path.open("rb") as stream:
                        body = stream.read()
                    async with self._client() as client:
                        await client.put_object(
                            Bucket=bucket_name,
                            Key=object_key,
                            Body=body,
                            ContentType=self._content_type_for(object_key),
                            CacheControl=self._cache_control_for(object_key),
                        )
                except BaseException as exc:
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    async with lock:
                        failures[relative_path] = _failure_message(exc)
                    return
                async with lock:
                    success_count += 1

        await asyncio.gather(*(upload_one(rel, path) for rel, path in files))
        failure_count = len(failures)
        return UploadDirResult(
            total_count=total,
            success_count=success_count,
            failure_count=failure_count,
            failures=failures,
        )

    async def read_file(self, *, bucket_name: str, filename: str) -> bytes:
        """读取对象的全部内容为 bytes；空对象返回 b""。

        对象不存在时抛出服务端错误。返回前释放响应连接；
        全量读取会占用与对象大小相应的内存。
        """
        object_key = _normalize_object_key(filename)
        async with self._client() as client:
            response = await client.get_object(Bucket=bucket_name, Key=object_key)
            body = response["Body"]
            try:
                return await body.read()
            finally:
                body.close()

    async def download_file(
        self,
        *,
        bucket_name: str,
        filename: str,
        target_file: str,
        overwrite: bool = False,
    ) -> None:
        """下载对象到本地 target_file，自动创建缺失的父目录。

        目标已存在且 overwrite=False 时抛出 FileExistsError；
        overwrite=True 时仅在下载完整成功后替换目标文件。
        失败时保留原目标文件，清理本次下载的临时文件并抛出异常。
        目标为目录时抛出 IsADirectoryError。
        """
        object_key = _normalize_object_key(filename)
        target = Path(target_file)
        if target.exists() and target.is_dir():
            raise IsADirectoryError(target_file)
        if target.exists() and not overwrite:
            raise FileExistsError(target_file)

        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(
            prefix=".minio-download-",
            dir=target.parent,
        )
        os.close(fd)
        temp = Path(temp_path)
        try:
            async with self._client() as client:
                response = await client.get_object(
                    Bucket=bucket_name,
                    Key=object_key,
                )
                body = response["Body"]
                try:
                    with temp.open("wb") as out:
                        while True:
                            chunk = await body.read(1024 * 1024)
                            if not chunk:
                                break
                            out.write(chunk)
                finally:
                    body.close()
            os.replace(temp, target)
        except BaseException:
            if temp.exists():
                temp.unlink()
            raise

    async def file_exists(self, *, bucket_name: str, filename: str) -> bool:
        """对象不存在时返回 False；桶不存在、权限或网络错误仍抛出。"""
        object_key = _normalize_object_key(filename)
        async with self._client() as client:
            try:
                await client.head_object(Bucket=bucket_name, Key=object_key)
            except ClientError as exc:
                if _is_not_found_error(exc):
                    return False
                raise
            return True

    async def list_files(self, bucket_name: str, prefix: str = "") -> list[str]:
        """返回匹配前缀的全部对象键，按对象键升序排列。

        prefix 允许为空，开头的 / 会被移除，采用字符串前缀匹配。
        包含所有分页及嵌套前缀，返回键不加开头的 /，无匹配返回 []。
        仅列举当前可见对象，不列举历史版本或删除标记。
        桶不存在时抛出服务端错误。
        """
        normalized_prefix = prefix.lstrip("/")
        keys: list[str] = []
        async with self._client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix=normalized_prefix,
            ):
                for item in page.get("Contents") or []:
                    key = item.get("Key")
                    if key is not None:
                        keys.append(key)
        keys.sort()
        return keys

    async def remove_file(self, *, bucket_name: str, filename: str) -> None:
        """删除指定对象；对象不存在时忽略，桶不存在时抛出服务端错误。

        开启版本控制时遵循服务端默认删除语义，不清除历史版本。
        """
        object_key = _normalize_object_key(filename)
        async with self._client() as client:
            await client.delete_object(Bucket=bucket_name, Key=object_key)

    def get_file_url(self, *, bucket_name: str, filename: str) -> str:
        """返回未签名的永久对象 URL，对对象键进行 URL 编码并保留 /。

        根据 secure 选择协议，不发起网络请求、不检查对象是否存在。
        URL 不授予权限；匿名访问需要桶策略允许公共读。
        """
        object_key = _normalize_object_key(filename)
        encoded_key = quote(object_key, safe="/")
        return f"{self._endpoint_url}/{bucket_name}/{encoded_key}"

    async def presigned_get_file_url(
        self,
        *,
        bucket_name: str,
        filename: str,
        expires: timedelta = timedelta(hours=1),
    ) -> str:
        """返回临时 GET 预签名 URL，不检查对象是否存在。

        expires 必须是 1 秒至 7 天之间的整数秒，否则抛出 ValueError。
        持有 URL 的人可在签名及签名所用凭据有效期间读取对象，
        实际有效期受凭据有效期和服务端权限约束；不修改桶策略。
        """
        object_key = _normalize_object_key(filename)
        expires_in = _expires_seconds(expires)
        async with self._client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expires_in,
            )

    async def presigned_put_file_url(
        self,
        *,
        bucket_name: str,
        filename: str,
        expires: timedelta = timedelta(hours=1),
    ) -> str:
        """返回临时 PUT 预签名 URL，用于直接上传指定对象。

        expires 必须是 1 秒至 7 天之间的整数秒，否则抛出 ValueError。
        不检查桶或对象是否存在，不创建桶、不上传内容、不修改桶策略。
        持有 URL 的人可在签名及签名所用凭据有效期间上传该对象，
        实际有效期受凭据有效期和服务端写入权限约束。
        调用方使用 HTTP PUT，将文件的二进制内容作为请求体，
        不使用 multipart/form-data；同名对象覆盖和版本约定与 write_file 相同。
        URL 在有效期内可重复使用，不是一次性上传凭证。
        不签入 Content-Type 或 Cache-Control，也不自动应用构造器中的策略；
        需要这些元数据时，由调用方在 PUT 请求中设置对应请求头。
        """
        object_key = _normalize_object_key(filename)
        expires_in = _expires_seconds(expires)
        async with self._client() as client:
            return await client.generate_presigned_url(
                "put_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expires_in,
            )
