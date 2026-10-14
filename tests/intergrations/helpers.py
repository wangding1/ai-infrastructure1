"""MinIO 与 deploy 集成测试辅助函数。"""

from pathlib import Path

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.utils.minio import MinIO
from deal_pilot.utils.sandbox import enter_sandbox
from support import langgraph_user_context, run_async


def minio_credentials() -> tuple[str, str, str, bool]:
    access_key = minio_settings.access_key.strip() or "root"
    secret_key = minio_settings.secret_key.strip() or "12345678"
    endpoint = minio_settings.endpoint.strip() or "localhost:9002"
    return access_key, secret_key, endpoint, minio_settings.secure


def make_minio_client(**kwargs) -> MinIO:
    access_key, secret_key, endpoint, secure = minio_credentials()
    return MinIO(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        **kwargs,
    )


async def remove_sandbox_paths(user_id: str, paths: list[str]) -> None:
    if not paths:
        return
    async with enter_sandbox(user_id) as sandbox:
        await sandbox.commands.run(f"rm -f -- {' '.join(paths)}")


async def write_sandbox_files(user_id: str, files: dict[str, bytes]) -> None:
    parent_dirs = sorted(
        {str(Path(path).parent) for path in files if "/" in path},
        key=len,
    )
    async with enter_sandbox(user_id) as sandbox:
        if parent_dirs:
            await sandbox.commands.run(f"mkdir -p -- {' '.join(parent_dirs)}")
        for path, data in files.items():
            await sandbox.files.write_file(path, data)


async def remove_object_prefix(client: MinIO, bucket: str, prefix: str) -> None:
    normalized = prefix.strip("/")
    if not normalized:
        return
    for key in await client.list_files(bucket, prefix=f"{normalized}/"):
        await client.remove_file(bucket_name=bucket, filename=key)
    if await client.file_exists(bucket_name=bucket, filename=normalized):
        await client.remove_file(bucket_name=bucket, filename=normalized)
