"""Deploy 集成测试 fixture（由 test_deploy.py 通过 pytest_plugins 加载）。"""

import os
import uuid
from datetime import timedelta

import pytest
from langgraph.store.memory import InMemoryStore
from opensandbox import Sandbox

from deal_pilot.config.nacos import nacos_config
import importlib

deploy_module = importlib.import_module("deal_pilot.tools.deploy")
from deal_pilot.utils import sandbox as sandbox_module
from deploy_harness import delete_test_bucket, new_test_bucket_name, sandbox_config_for_deploy
from helpers import run_async


@pytest.fixture(scope="module")
def deploy_sandbox_image() -> str:
    image = os.environ.get("DEPLOY_SANDBOX_IMAGE", "").strip()
    if not image:
        pytest.fail("请设置 DEPLOY_SANDBOX_IMAGE（项目 docker/sandbox/Dockerfile 构建的镜像）")
    return image


@pytest.fixture
def deploy_store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def deploy_bucket(monkeypatch: pytest.MonkeyPatch) -> str:
    name = new_test_bucket_name()
    monkeypatch.setattr(deploy_module, "_BUCKET_NAME", name)
    yield name
    run_async(delete_test_bucket(name))


@pytest.fixture
def deploy_patched_sandbox(
    deploy_sandbox_image: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = sandbox_config_for_deploy(deploy_sandbox_image)

    async def _get_config(_name, _schema):
        return config

    monkeypatch.setattr(nacos_config, "get_config", _get_config)

    async def _create_ephemeral(
        *,
        sandbox_config,
        connection_config,
        key: str,
        store,
    ) -> Sandbox:
        sandbox = await Sandbox.create(
            image=sandbox_config.image,
            connection_config=connection_config,
            timeout=timedelta(seconds=sandbox_config.timeout_seconds),
            ready_timeout=timedelta(seconds=sandbox_config.ready_timeout_seconds),
            volumes=[],
        )
        await store.aput(
            sandbox_module._NAMESPACE,
            key,
            {"sandbox_id": sandbox.id},
        )
        return sandbox

    monkeypatch.setattr(sandbox_module, "_create", _create_ephemeral)


@pytest.fixture
def deploy_user(deploy_store: InMemoryStore):
    user_id = f"deploy-it-{uuid.uuid4().hex[:10]}"
    yield user_id, deploy_store
    from deploy_harness import terminate_user_sandbox

    run_async(terminate_user_sandbox(user_id, deploy_store))
