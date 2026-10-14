"""MinIO 客户端集成测试（真实连接 shared-minio 并上传对象）。"""

import io
import uuid
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from botocore.exceptions import ClientError
from helpers import make_minio_client, minio_credentials, run_async

from deal_pilot.utils.minio import (
    CacheStrategy,
    ContentTypeStrategy,
    MinIO,
    UploadDirResult,
)


class TestMinIOInit:
    def test_rejects_endpoint_with_protocol(self):
        with pytest.raises(ValueError, match="协议"):
            MinIO(endpoint="http://localhost:9002", access_key="a", secret_key="b")

    def test_rejects_invalid_endpoint(self):
        with pytest.raises(ValueError, match="endpoint"):
            MinIO(endpoint="", access_key="a", secret_key="b")

    def test_rejects_empty_keys(self):
        with pytest.raises(ValueError, match="access_key"):
            MinIO(endpoint="localhost:9002", access_key="  ", secret_key="b")
        with pytest.raises(ValueError, match="secret_key"):
            MinIO(endpoint="localhost:9002", access_key="a", secret_key="")

    def test_rejects_invalid_cache_and_content_type(self):
        with pytest.raises(ValueError):
            MinIO(
                endpoint="localhost:9002",
                access_key="a",
                secret_key="b",
                cache_strategy=[CacheStrategy(extensions=["bad"], max_age=-1)],
            )
        with pytest.raises(ValueError):
            MinIO(
                endpoint="localhost:9002",
                access_key="a",
                secret_key="b",
                content_type_strategy=ContentTypeStrategy(
                    extensions={".png": "image/png", ".PNG": "image/x-png"}
                ),
            )


class TestMinIOBucket:
    def test_create_exists_and_remove(self, minio_client: MinIO):
        name = f"deal-pilot-bucket-{uuid.uuid4().hex[:10]}"

        async def _run() -> None:
            assert await minio_client.bucket_exists(name) is False
            await minio_client.create_bucket(name, allow_public_read=False)
            assert await minio_client.bucket_exists(name) is True
            await minio_client.create_bucket(name, allow_public_read=False)
            await minio_client.remove_bucket(name)
            assert await minio_client.bucket_exists(name) is False
            await minio_client.remove_bucket(name)

        run_async(_run())

    def test_remove_bucket_force_clears_objects(self, minio_client: MinIO):
        name = f"deal-pilot-force-{uuid.uuid4().hex[:10]}"

        async def _run() -> None:
            await minio_client.create_bucket(name)
            await minio_client.write_file(
                bucket_name=name,
                filename="a.txt",
                stream=io.BytesIO(b"x"),
            )
            await minio_client.remove_bucket(name, force=True)
            assert await minio_client.bucket_exists(name) is False

        run_async(_run())

    def test_remove_non_empty_bucket_without_force_raises(self, minio_client: MinIO):
        name = f"deal-pilot-nonempty-{uuid.uuid4().hex[:10]}"

        async def _run() -> None:
            await minio_client.create_bucket(name, allow_public_read=False)
            await minio_client.write_file(
                bucket_name=name,
                filename="x",
                stream=io.BytesIO(b"1"),
            )
            with pytest.raises(ClientError):
                await minio_client.remove_bucket(name, force=False)
            await minio_client.remove_bucket(name, force=True)

        run_async(_run())


class TestMinIOObjects:
    def test_write_read_upload_download_roundtrip(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        payload = b"hello-minio-\xff\xfe"
        local_src = tmp_path / "src.bin"
        local_src.write_bytes(payload)
        download_path = tmp_path / "out" / "downloaded.bin"

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="/nested/stream.bin",
                stream=io.BytesIO(payload),
            )
            assert await minio_client.read_file(
                bucket_name=bucket, filename="nested/stream.bin"
            ) == payload

            await minio_client.upload_file(
                bucket_name=bucket,
                filename="from-disk.bin",
                source_file=str(local_src),
            )
            assert (
                await minio_client.read_file(
                    bucket_name=bucket, filename="from-disk.bin"
                )
                == payload
            )

            assert await minio_client.file_exists(
                bucket_name=bucket, filename="from-disk.bin"
            )
            assert await minio_client.file_exists(
                bucket_name=bucket, filename="missing.bin"
            ) is False

            await minio_client.download_file(
                bucket_name=bucket,
                filename="from-disk.bin",
                target_file=str(download_path),
            )
            assert download_path.read_bytes() == payload

            await minio_client.write_file(
                bucket_name=bucket,
                filename="empty",
                stream=io.BytesIO(b""),
            )
            assert (
                await minio_client.read_file(bucket_name=bucket, filename="empty")
                == b""
            )

            keys = await minio_client.list_files(bucket)
            assert keys == sorted(keys)
            assert "from-disk.bin" in keys
            assert "nested/stream.bin" in keys

            await minio_client.remove_file(
                bucket_name=bucket, filename="from-disk.bin"
            )
            assert await minio_client.file_exists(
                bucket_name=bucket, filename="from-disk.bin"
            ) is False
            await minio_client.remove_file(
                bucket_name=bucket, filename="from-disk.bin"
            )

        run_async(_run())
        assert download_path.read_bytes() == payload

    def test_list_files_prefix(self, minio_client: MinIO, bucket: str):
        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="alpha/one.txt",
                stream=io.BytesIO(b"1"),
            )
            await minio_client.write_file(
                bucket_name=bucket,
                filename="beta/two.txt",
                stream=io.BytesIO(b"2"),
            )
            listed = await minio_client.list_files(bucket, prefix="alpha/")
            assert listed == ["alpha/one.txt"]

        run_async(_run())

    def test_download_file_exists_and_directory_errors(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        target = tmp_path / "exists.bin"
        target.write_bytes(b"old")
        dir_target = tmp_path / "adir"
        dir_target.mkdir()

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="obj.bin",
                stream=io.BytesIO(b"new"),
            )
            with pytest.raises(FileExistsError):
                await minio_client.download_file(
                    bucket_name=bucket,
                    filename="obj.bin",
                    target_file=str(target),
                )
            with pytest.raises(IsADirectoryError):
                await minio_client.download_file(
                    bucket_name=bucket,
                    filename="obj.bin",
                    target_file=str(dir_target),
                )
            await minio_client.download_file(
                bucket_name=bucket,
                filename="obj.bin",
                target_file=str(target),
                overwrite=True,
            )

        run_async(_run())
        assert target.read_bytes() == b"new"

    def test_read_missing_object_raises(self, minio_client: MinIO, bucket: str):
        async def _run() -> None:
            with pytest.raises(ClientError):
                await minio_client.read_file(
                    bucket_name=bucket, filename="no-such-key"
                )

        run_async(_run())

    def test_upload_file_errors(self, minio_client: MinIO, bucket: str, tmp_path: Path):
        async def _run() -> None:
            with pytest.raises(FileNotFoundError):
                await minio_client.upload_file(
                    bucket_name=bucket,
                    filename="x",
                    source_file=str(tmp_path / "missing"),
                )
            d = tmp_path / "adir"
            d.mkdir()
            with pytest.raises(IsADirectoryError):
                await minio_client.upload_file(
                    bucket_name=bucket,
                    filename="x",
                    source_file=str(d),
                )

        run_async(_run())

    def test_invalid_filename_raises(self, minio_client: MinIO, bucket: str):
        async def _run() -> None:
            with pytest.raises(ValueError):
                await minio_client.write_file(
                    bucket_name=bucket,
                    filename="///",
                    stream=io.BytesIO(b"a"),
                )

        run_async(_run())

    def test_filename_strips_leading_slash_only(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：filename 开头的 / 会被移除。"""
        stored_key = "nested/leading.txt"
        payload = b"leading-slash"

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename=f"/{stored_key}",
                stream=io.BytesIO(payload),
            )
            assert await minio_client.file_exists(
                bucket_name=bucket, filename=stored_key
            )
            assert (
                await minio_client.read_file(
                    bucket_name=bucket, filename=stored_key
                )
                == payload
            )

        run_async(_run())

    def test_write_file_reads_from_stream_current_position(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：从二进制流当前位置读取到结尾。"""
        stream = io.BytesIO(b"SKIP-TAIL")
        stream.seek(5)

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="from-offset.bin",
                stream=stream,
            )
            assert (
                await minio_client.read_file(
                    bucket_name=bucket, filename="from-offset.bin"
                )
                == b"TAIL"
            )

        run_async(_run())

    def test_write_file_overwrites_existing_object(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：同名对象覆盖当前内容。"""
        key = "overwrite/me.txt"

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename=key,
                stream=io.BytesIO(b"v1"),
            )
            await minio_client.write_file(
                bucket_name=bucket,
                filename=key,
                stream=io.BytesIO(b"v2"),
            )
            assert (
                await minio_client.read_file(bucket_name=bucket, filename=key)
                == b"v2"
            )

        run_async(_run())

    def test_methods_other_than_create_bucket_do_not_create_bucket(
        self, minio_client: MinIO
    ):
        """约定：除 create_bucket 外不自动创建桶。"""
        missing = f"never-created-{uuid.uuid4().hex[:10]}"

        async def _run() -> None:
            assert await minio_client.bucket_exists(missing) is False
            with pytest.raises(ClientError):
                await minio_client.write_file(
                    bucket_name=missing,
                    filename="a",
                    stream=io.BytesIO(b"x"),
                )

        run_async(_run())

    def test_list_files_on_missing_bucket_raises(self, minio_client: MinIO):
        """list_files：桶不存在时抛出服务端错误。"""
        missing = f"no-bucket-{uuid.uuid4().hex[:10]}"

        async def _run() -> None:
            with pytest.raises(ClientError):
                await minio_client.list_files(missing)

        run_async(_run())

    def test_list_files_no_match_returns_empty_list(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：无匹配返回 []。"""
        async def _run() -> None:
            assert await minio_client.list_files(bucket, prefix="nope/") == []

        run_async(_run())

    def test_list_files_strips_leading_slash_on_prefix(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：prefix 开头的 / 会被移除。"""
        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="p/item",
                stream=io.BytesIO(b"i"),
            )
            assert await minio_client.list_files(bucket, prefix="/p/") == [
                "p/item"
            ]

        run_async(_run())

    def test_download_file_creates_missing_parent_directories(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        """约定：自动创建缺失的父目录。"""
        target = tmp_path / "deep" / "nested" / "out.dat"

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="dl.bin",
                stream=io.BytesIO(b"data"),
            )
            await minio_client.download_file(
                bucket_name=bucket,
                filename="dl.bin",
                target_file=str(target),
            )

        run_async(_run())
        assert target.read_bytes() == b"data"


class TestMinIOUploadDir:
    def test_upload_dir_structure_prefix_and_hidden(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        root = tmp_path / "tree"
        (root / "sub").mkdir(parents=True)
        (root / "sub" / "visible.txt").write_text("v", encoding="utf-8")
        (root / ".hidden").write_text("h", encoding="utf-8")
        (root / "link.txt").symlink_to(root / "sub" / "visible.txt")

        async def _run() -> None:
            result = await minio_client.upload_dir(
                bucket_name=bucket,
                source_dir=str(root),
                prefix="/assets/",
                max_concurrency=2,
            )
            assert result == UploadDirResult(
                total_count=2,
                success_count=2,
                failure_count=0,
                failures={},
            )
            keys = await minio_client.list_files(bucket, prefix="assets/")
            assert sorted(keys) == [
                "assets/.hidden",
                "assets/sub/visible.txt",
            ]

        run_async(_run())

    def test_upload_dir_empty_directory(self, minio_client: MinIO, bucket: str, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()

        async def _run() -> None:
            result = await minio_client.upload_dir(
                bucket_name=bucket, source_dir=str(empty)
            )
            assert result.total_count == 0
            assert result.success_count == 0
            assert result.failure_count == 0
            assert result.failures == {}

        run_async(_run())

    def test_upload_dir_validation(self, minio_client: MinIO, bucket: str, tmp_path: Path):
        async def _run() -> None:
            with pytest.raises(ValueError):
                await minio_client.upload_dir(
                    bucket_name=bucket,
                    source_dir=str(tmp_path),
                    max_concurrency=0,
                )
            with pytest.raises(FileNotFoundError):
                await minio_client.upload_dir(
                    bucket_name=bucket,
                    source_dir=str(tmp_path / "nope"),
                )
            f = tmp_path / "file.txt"
            f.write_text("x", encoding="utf-8")
            with pytest.raises(NotADirectoryError):
                await minio_client.upload_dir(
                    bucket_name=bucket, source_dir=str(f)
                )

        run_async(_run())

    def test_upload_dir_records_failures_for_missing_bucket(
        self, minio_client: MinIO, tmp_path: Path
    ):
        root = tmp_path / "one"
        root.mkdir()
        (root / "a.txt").write_text("a", encoding="utf-8")

        async def _run() -> None:
            missing = f"no-bucket-{uuid.uuid4().hex[:8]}"
            result = await minio_client.upload_dir(
                bucket_name=missing,
                source_dir=str(root),
            )
            assert result.total_count == 1
            assert result.success_count == 0
            assert result.failure_count == 1
            assert "a.txt" in result.failures

        run_async(_run())

    def test_upload_dir_does_not_delete_unrelated_objects(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        """约定：不删除桶内未包含在本地目录中的对象。"""
        root = tmp_path / "batch"
        root.mkdir()
        (root / "only.txt").write_text("o", encoding="utf-8")

        async def _run() -> None:
            await minio_client.write_file(
                bucket_name=bucket,
                filename="orphan/stays",
                stream=io.BytesIO(b"stay"),
            )
            await minio_client.upload_dir(
                bucket_name=bucket,
                source_dir=str(root),
                prefix="batch",
            )
            assert await minio_client.file_exists(
                bucket_name=bucket, filename="orphan/stays"
            )
            assert await minio_client.file_exists(
                bucket_name=bucket, filename="batch/only.txt"
            )

        run_async(_run())

    def test_upload_dir_result_counts_match_docstring(
        self, minio_client: MinIO, bucket: str, tmp_path: Path
    ):
        """UploadDirResult：total = success + failure，failure_count = len(failures)。"""
        root = tmp_path / "counts"
        root.mkdir()
        (root / "ok.txt").write_text("1", encoding="utf-8")

        async def _run() -> None:
            result = await minio_client.upload_dir(
                bucket_name=bucket, source_dir=str(root)
            )
            assert result.total_count == result.success_count + result.failure_count
            assert result.failure_count == len(result.failures)

        run_async(_run())


class TestMinIOUrlsAndPolicies:
    def test_get_file_url_encoding(self, minio_client: MinIO, bucket: str):
        url = minio_client.get_file_url(
            bucket_name=bucket,
            filename="path/a b.txt",
        )
        assert "%20" in url
        assert url.endswith(f"{bucket}/path/a%20b.txt") or "a%20b.txt" in url

    def test_get_file_url_uses_scheme_from_secure_flag(self):
        """约定：根据 secure 选择协议；不发起网络请求。"""
        client = make_minio_client()
        url = client.get_file_url(bucket_name="b", filename="k")
        _, _, endpoint, secure = minio_credentials()
        if secure:
            assert url.startswith("https://")
        else:
            assert url.startswith("http://")
        assert f"{endpoint}/b/k" in url

    def test_presigned_get_and_put(self, minio_client: MinIO, bucket: str):
        body = b"presigned-payload"

        async def _run() -> None:
            put_url = await minio_client.presigned_put_file_url(
                bucket_name=bucket,
                filename="presigned/obj.bin",
                expires=timedelta(seconds=120),
            )
            async with httpx.AsyncClient() as http:
                put_resp = await http.put(put_url, content=body)
                put_resp.raise_for_status()

                get_url = await minio_client.presigned_get_file_url(
                    bucket_name=bucket,
                    filename="presigned/obj.bin",
                    expires=timedelta(minutes=5),
                )
                get_resp = await http.get(get_url)
                get_resp.raise_for_status()
                assert get_resp.content == body

        run_async(_run())

    def test_presigned_expires_validation(self, minio_client: MinIO, bucket: str):
        async def _run() -> None:
            with pytest.raises(ValueError):
                await minio_client.presigned_get_file_url(
                    bucket_name=bucket,
                    filename="x",
                    expires=timedelta(seconds=0),
                )
            with pytest.raises(ValueError, match="整数秒"):
                await minio_client.presigned_put_file_url(
                    bucket_name=bucket,
                    filename="x",
                    expires=timedelta(seconds=1, milliseconds=500),
                )
            with pytest.raises(ValueError, match="7 天"):
                await minio_client.presigned_get_file_url(
                    bucket_name=bucket,
                    filename="x",
                    expires=timedelta(days=8),
                )

        run_async(_run())

    def test_presigned_put_url_reusable_for_overwrite(
        self, minio_client: MinIO, bucket: str
    ):
        """约定：预签名 PUT 在有效期内可重复使用，同名覆盖。"""
        key = "presigned/reuse.bin"

        async def _run() -> None:
            url = await minio_client.presigned_put_file_url(
                bucket_name=bucket,
                filename=key,
                expires=timedelta(minutes=10),
            )
            async with httpx.AsyncClient() as http:
                r1 = await http.put(url, content=b"first")
                r1.raise_for_status()
                r2 = await http.put(url, content=b"second")
                r2.raise_for_status()
            assert (
                await minio_client.read_file(bucket_name=bucket, filename=key)
                == b"second"
            )

        run_async(_run())

    def test_create_bucket_without_public_read_denies_anonymous_get(
        self, minio_client: MinIO
    ):
        """allow_public_read=False 时不新增公共读；匿名 GET 不应成功。"""
        name = f"deal-pilot-private-{uuid.uuid4().hex[:10]}"
        key = "secret.txt"

        async def _run() -> None:
            await minio_client.create_bucket(name, allow_public_read=False)
            await minio_client.write_file(
                bucket_name=name,
                filename=key,
                stream=io.BytesIO(b"hidden"),
            )
            url = minio_client.get_file_url(bucket_name=name, filename=key)
            async with httpx.AsyncClient() as http:
                resp = await http.get(url)
                assert resp.status_code in (401, 403)
            assert (
                await minio_client.read_file(bucket_name=name, filename=key)
                == b"hidden"
            )
            await minio_client.remove_bucket(name, force=True)

        run_async(_run())

    def test_public_read_allows_anonymous_get(self, bucket: str):
        client = make_minio_client()
        key = "public/read.txt"
        content = b"public-body"

        async def _run() -> None:
            await client.write_file(
                bucket_name=bucket,
                filename=key,
                stream=io.BytesIO(content),
            )
            url = client.get_file_url(bucket_name=bucket, filename=key)
            async with httpx.AsyncClient() as http:
                resp = await http.get(url)
                resp.raise_for_status()
                assert resp.content == content

        run_async(_run())


class TestMinIOContentTypeByTargetKey:
    def test_upload_file_uses_target_filename_for_content_type(
        self, bucket: str, tmp_path: Path
    ):
        """upload_file：Content-Type 按目标 filename 决定，不按本地路径。"""
        local = tmp_path / "data.bin"
        local.write_bytes(b"x")
        client = make_minio_client(
            content_type_strategy=ContentTypeStrategy(
                extensions={".html": "text/html; charset=utf-8"},
            ),
        )

        async def _run() -> None:
            await client.upload_file(
                bucket_name=bucket,
                filename="page.html",
                source_file=str(local),
            )
            url = client.get_file_url(bucket_name=bucket, filename="page.html")
            async with httpx.AsyncClient() as http:
                head = await http.head(url)
                head.raise_for_status()
                assert head.headers.get("content-type", "").startswith(
                    "text/html"
                )

        run_async(_run())


class TestMinIOMetadata:
    def test_cache_control_and_content_type_on_upload(
        self, bucket: str, tmp_path: Path
    ):
        client = make_minio_client(
            cache_strategy=[
                CacheStrategy(extensions=[".html"], max_age=3600),
                CacheStrategy(extensions=[".png"], max_age=0),
            ],
            content_type_strategy=ContentTypeStrategy(
                default_content_type="application/octet-stream",
                extensions={
                    ".html": "text/html; charset=utf-8",
                    ".gz": "application/gzip",
                },
            ),
        )
        key_html = "pages/index.html"
        key_gz = "archive.tar.gz"
        key_png = "img/logo.png"

        async def _run() -> None:
            await client.write_file(
                bucket_name=bucket,
                filename=key_html,
                stream=io.BytesIO(b"<p>hi</p>"),
            )
            await client.write_file(
                bucket_name=bucket,
                filename=key_gz,
                stream=io.BytesIO(b"gz"),
            )
            await client.write_file(
                bucket_name=bucket,
                filename=key_png,
                stream=io.BytesIO(b"\x89PNG"),
            )

            async with client._client() as s3:
                h = await s3.head_object(Bucket=bucket, Key=key_html)
                assert h["ContentType"] == "text/html; charset=utf-8"
                assert h.get("CacheControl") == "max-age=3600"

                g = await s3.head_object(Bucket=bucket, Key=key_gz)
                assert g["ContentType"] == "application/gzip"
                assert g.get("CacheControl") == "no-store"

                p = await s3.head_object(Bucket=bucket, Key=key_png)
                assert p.get("CacheControl") == "no-store"

        run_async(_run())

    def test_cache_strategy_later_match_overrides_earlier(self, bucket: str):
        """约定：多个缓存策略匹配时后者覆盖前者。"""
        client = make_minio_client(
            cache_strategy=[
                CacheStrategy(extensions=[".css"], max_age=100),
                CacheStrategy(extensions=[".css"], max_age=200),
            ],
        )
        key = "s.css"

        async def _run() -> None:
            await client.write_file(
                bucket_name=bucket,
                filename=key,
                stream=io.BytesIO(b"body"),
            )
            url = client.get_file_url(bucket_name=bucket, filename=key)
            async with httpx.AsyncClient() as http:
                head = await http.head(url)
                head.raise_for_status()
                assert head.headers.get("cache-control") == "max-age=200"

        run_async(_run())
