"""MinIO 集成测试辅助函数。"""

import asyncio

from deal_pilot.config.minio_settings import minio_settings
from deal_pilot.utils.minio import MinIO


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


def run_async(coro):
    return asyncio.run(coro)
