"""MinIO 集成测试公共配置（需 shared-minio 与 MINIO_INTEGRATION=1）。"""

import os
import uuid

import pytest

from deal_pilot.utils.minio import MinIO

from helpers import make_minio_client, run_async

pytestmark = pytest.mark.skipif(
    os.environ.get("MINIO_INTEGRATION") != "1",
    reason="设置 MINIO_INTEGRATION=1 并启动 docker compose 中的 shared-minio",
)


@pytest.fixture
def minio_client() -> MinIO:
    return make_minio_client()


@pytest.fixture
def bucket(minio_client: MinIO) -> str:
    name = f"deal-pilot-it-{uuid.uuid4().hex[:12]}"

    async def _setup() -> None:
        await minio_client.create_bucket(name)

    async def _teardown() -> None:
        await minio_client.remove_bucket(name, force=True)

    run_async(_setup())
    try:
        yield name
    finally:
        run_async(_teardown())
